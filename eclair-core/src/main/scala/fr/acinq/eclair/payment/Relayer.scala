/*
 * Copyright 2019 ACINQ SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.acinq.eclair.payment

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.event.LoggingAdapter
import fr.acinq.bitcoin.ByteVector32
import fr.acinq.bitcoin.Crypto.{PrivateKey, PublicKey}
import fr.acinq.eclair.channel._
import fr.acinq.eclair.crypto.Sphinx
import fr.acinq.eclair.db.{OutgoingPayment, OutgoingPaymentStatus}
import fr.acinq.eclair.payment.PaymentInitiator.SendPaymentConfig
import fr.acinq.eclair.router.{Announcements, Router}
import fr.acinq.eclair.wire.Onion.FinalLegacyPayload
import fr.acinq.eclair.wire.OnionTlv.TrampolineOnion
import fr.acinq.eclair.wire._
import fr.acinq.eclair.{CltvExpiryDelta, Features, LongToBtcAmount, MilliSatoshi, NodeParams, ShortChannelId, UInt64, nodeFee}
import grizzled.slf4j.Logging
import scodec.bits.ByteVector
import scodec.{Attempt, DecodeResult}

import scala.collection.mutable

// @formatter:off
sealed trait Origin
case class Local(id: UUID, sender: Option[ActorRef]) extends Origin // we don't persist reference to local actors
case class Relayed(originChannelId: ByteVector32, originHtlcId: Long, amountIn: MilliSatoshi, amountOut: MilliSatoshi) extends Origin

sealed trait ForwardMessage
case class ForwardAdd(add: UpdateAddHtlc, previousFailures: Seq[AddHtlcFailed] = Seq.empty) extends ForwardMessage
case class ForwardFulfill(fulfill: UpdateFulfillHtlc, to: Origin, htlc: UpdateAddHtlc) extends ForwardMessage
case class ForwardFail(fail: UpdateFailHtlc, to: Origin, htlc: UpdateAddHtlc) extends ForwardMessage
case class ForwardFailMalformed(fail: UpdateFailMalformedHtlc, to: Origin, htlc: UpdateAddHtlc) extends ForwardMessage

case object GetUsableBalances
case class UsableBalance(remoteNodeId: PublicKey, shortChannelId: ShortChannelId, canSend: MilliSatoshi, canReceive: MilliSatoshi, isPublic: Boolean, lastUpdate: ChannelUpdate) {
  override def toString = s"Channel($shortChannelId, $remoteNodeId, $canSend, $canReceive, $isPublic)"
}
case class UsableBalances(balances: Seq[UsableBalance])
// @formatter:on

/**
 * Created by PM on 01/02/2017.
 */
class Relayer(nodeParams: NodeParams, router: ActorRef, register: ActorRef, paymentHandler: ActorRef) extends Actor with ActorLogging {

  import Relayer._

  // we pass these to helpers classes so that they have the logging context
  implicit def implicitLog: LoggingAdapter = log

  context.system.eventStream.subscribe(self, classOf[LocalChannelUpdate])
  context.system.eventStream.subscribe(self, classOf[LocalChannelDown])
  context.system.eventStream.subscribe(self, classOf[AvailableBalanceChanged])

  private val commandBuffer = context.actorOf(Props(new CommandBuffer(nodeParams, register)))
  private var payments = Map.empty[UUID, UpdateAddHtlc]

  override def receive: Receive = main(Map.empty, new mutable.HashMap[PublicKey, mutable.Set[ShortChannelId]] with mutable.MultiMap[PublicKey, ShortChannelId])

  def main(channelUpdates: Map[ShortChannelId, OutgoingChannel], node2channels: mutable.HashMap[PublicKey, mutable.Set[ShortChannelId]] with mutable.MultiMap[PublicKey, ShortChannelId]): Receive = {
    case GetUsableBalances =>
      sender ! UsableBalances(channelUpdates.values
        .filter(o => Announcements.isEnabled(o.channelUpdate.channelFlags))
        .map(o => UsableBalance(
          remoteNodeId = o.nextNodeId,
          shortChannelId = o.channelUpdate.shortChannelId,
          canSend = o.commitments.availableBalanceForSend,
          canReceive = o.commitments.availableBalanceForReceive,
          isPublic = o.commitments.announceChannel,
          lastUpdate = o.channelUpdate)).toSeq)

    case LocalChannelUpdate(_, channelId, shortChannelId, remoteNodeId, _, channelUpdate, commitments) =>
      log.debug(s"updating local channel info for channelId=$channelId shortChannelId=$shortChannelId remoteNodeId=$remoteNodeId channelUpdate={} commitments={}", channelUpdate, commitments)
      val channelUpdates1 = channelUpdates + (channelUpdate.shortChannelId -> OutgoingChannel(remoteNodeId, channelUpdate, commitments))
      context become main(channelUpdates1, node2channels.addBinding(remoteNodeId, channelUpdate.shortChannelId))

    case LocalChannelDown(_, channelId, shortChannelId, remoteNodeId) =>
      log.debug(s"removed local channel info for channelId=$channelId shortChannelId=$shortChannelId")
      context become main(channelUpdates - shortChannelId, node2channels.removeBinding(remoteNodeId, shortChannelId))

    case AvailableBalanceChanged(_, _, shortChannelId, _, commitments) =>
      val channelUpdates1 = channelUpdates.get(shortChannelId) match {
        case Some(c: OutgoingChannel) => channelUpdates + (shortChannelId -> c.copy(commitments = commitments))
        case None => channelUpdates // we only consider the balance if we have the channel_update
      }
      context become main(channelUpdates1, node2channels)

    case ForwardAdd(add, previousFailures) =>
      log.debug(s"received forwarding request for htlc #${add.id} paymentHash=${add.paymentHash} from channelId=${add.channelId}")
      decryptPacket(add, nodeParams.privateKey, nodeParams.globalFeatures) match {
        case Right(p: FinalPayload) =>
          validateFinal(p) match {
            case Some(cmdFail) =>
              log.info(s"rejecting htlc #${add.id} paymentHash=${add.paymentHash} from channelId=${add.channelId} reason=${cmdFail.reason}")
              commandBuffer ! CommandBuffer.CommandSend(add.channelId, add.id, cmdFail)
            case None =>
              log.debug(s"forwarding htlc #${add.id} paymentHash=${add.paymentHash} to payment-handler")
              paymentHandler forward p
          }
        case Right(r: RelayPayload) =>
          handleRelay(r, channelUpdates, node2channels, previousFailures, nodeParams.chainHash) match {
            case RelayFailure(cmdFail) =>
              log.info(s"rejecting htlc #${add.id} paymentHash=${add.paymentHash} from channelId=${add.channelId} to shortChannelId=${r.payload.outgoingChannelId} reason=${cmdFail.reason}")
              commandBuffer ! CommandBuffer.CommandSend(add.channelId, add.id, cmdFail)
            case RelaySuccess(selectedShortChannelId, cmdAdd) =>
              log.info(s"forwarding htlc #${add.id} paymentHash=${add.paymentHash} from channelId=${add.channelId} to shortChannelId=$selectedShortChannelId")
              register ! Register.ForwardShortId(selectedShortChannelId, cmdAdd)
          }
        case Right(t: TrampolinePayload) =>
          val valid = validateTrampoline(t)
          if (!nodeParams.enableTrampolineRouting) {
            log.info(s"rejecting htlc #${add.id} paymentHash=${add.paymentHash} from channelId=${add.channelId}: trampoline routing disabled")
            commandBuffer ! CommandBuffer.CommandSend(add.channelId, add.id, CMD_FAIL_HTLC(add.id, Right(PermanentNodeFailure), commit = true))
          } else if (valid.isDefined) {
            log.info(s"rejecting htlc #${add.id} paymentHash=${add.paymentHash} from channelId=${add.channelId}: invalid trampoline payload")
            commandBuffer ! CommandBuffer.CommandSend(add.channelId, add.id, valid.get)
          } else {
            log.info(s"forwarding htlc #${add.id} paymentHash=${add.paymentHash} from channelId=${add.channelId} using trampoline routing")
            // TODO: @t-bast: to estimate the fee and cltv, we need to scan the latest channel updates for all our outgoing channels and choose the highest
            // Need to clarify that with tests to observe behavior
            val routeMaxExpiry = add.cltvExpiry - nodeParams.expiryDeltaBlocks.toCltvExpiry(nodeParams.currentBlockHeight)
            val routeMaxFee = add.amountMsat - t.nextPayload.amountToForward - nodeFee(nodeParams.feeBase, nodeParams.feeProportionalMillionth, t.nextPayload.amountToForward)
            val routeParams = Router.getDefaultRouteParams(nodeParams.routerConf).copy(maxFeeBase = routeMaxFee, maxFeePct = routeMaxFee.toLong.toDouble / t.nextPayload.amountToForward.toLong, routeMaxCltv = routeMaxExpiry)

            val id = UUID.randomUUID()
            val payCfg = SendPaymentConfig(id, id, None, t.add.paymentHash, t.nextPayload.outgoingNodeId, None, storeInDb = false, publishEvent = false)
            val payFSM = context.actorOf(PaymentLifecycle.props(nodeParams, payCfg, router, register))
            val finalPayload = t.nextPayload.records.get[OnionTlv.PaymentSecret] match {
              case Some(OnionTlv.PaymentSecret(secret)) => Onion.createMultiPartPayload(
                t.nextPayload.amountToForward,
                t.nextPayload.records.get[OnionTlv.TotalAmount].map(_.amount).getOrElse(t.nextPayload.amountToForward),
                t.nextPayload.outgoingCltv,
                secret)
              case None => FinalLegacyPayload(t.nextPayload.amountToForward, t.nextPayload.outgoingCltv)
            }
            val payment = PaymentLifecycle.SendPayment(t.add.paymentHash, t.nextPayload.outgoingNodeId, finalPayload, nodeParams.maxPaymentAttempts, routeParams = Some(routeParams))
            payFSM ! payment
            payments = payments + (id -> t.add)

            // Tests to cover:
            //  - relay a trampoline htlc-add with a channel to the next trampoline node
            //  - relay a trampoline htlc-add finds a route to the next trampoline node
            //  - relay a trampoline htlc-add to a legacy recipient
            //  - relay a trampoline htlc-add with retries
            //  - fail to relay a trampoline htlc-add when relay fee isn't sufficient
            //  - fail to relay a trampoline htlc-add when relay expiry isn't sufficient
            //  - forward trampoline htlc-fulfill
            //  - forward trampoline htlc-fail
          }
        case Left(badOnion: BadOnion) =>
          log.warning(s"couldn't parse onion: reason=${badOnion.message}")
          val cmdFail = CMD_FAIL_MALFORMED_HTLC(add.id, badOnion.onionHash, badOnion.code, commit = true)
          log.info(s"rejecting htlc #${add.id} paymentHash=${add.paymentHash} from channelId=${add.channelId} reason=malformed onionHash=${cmdFail.onionHash} failureCode=${cmdFail.failureCode}")
          commandBuffer ! CommandBuffer.CommandSend(add.channelId, add.id, cmdFail)
        case Left(failure) =>
          log.warning(s"couldn't process onion: reason=${failure.message}")
          val cmdFail = CMD_FAIL_HTLC(add.id, Right(failure), commit = true)
          commandBuffer ! CommandBuffer.CommandSend(add.channelId, add.id, cmdFail)
      }

    case p@PaymentSent(id, _, paymentPreimage, _) => payments.get(id).foreach(add => {
      // TODO: @t-bast: we should instead plug this to the Relayer's ForwardFulfill flow with a Relayed payment
      // This should allow us to log the correct value for toChannelId
      // TODO: it also seems the entry is still there in pending relays -> make sure it's deleted properly
      val cmd = CMD_FULFILL_HTLC(add.id, paymentPreimage, commit = true)
      commandBuffer ! CommandBuffer.CommandSend(add.channelId, add.id, cmd)
      context.system.eventStream.publish(PaymentRelayed(add.amountMsat, p.amount + p.feesPaid, add.paymentHash, fromChannelId = add.channelId, toChannelId = add.channelId))
      payments = payments - id
    })

    case PaymentFailed(id, _, _, _) => payments.get(id).foreach(add => {
      log.info(s"rejecting htlc #${add.id} paymentHash=${add.paymentHash} from channelId=${add.channelId} reason=failed to relay via trampoline")
      // TODO: @t-bast: need to translate the payment failures into the right FailureMessage (not always TemporaryNodeFailure)
      // Need tests to see how this behaves when integrated
      commandBuffer ! CommandBuffer.CommandSend(add.channelId, add.id, CMD_FAIL_HTLC(add.id, Right(TemporaryNodeFailure), commit = true))
      payments = payments - id
    })

    case Status.Failure(Register.ForwardShortIdFailure(Register.ForwardShortId(shortChannelId, CMD_ADD_HTLC(_, _, _, _, Right(add), _, _)))) =>
      log.warning(s"couldn't resolve downstream channel $shortChannelId, failing htlc #${add.id}")
      val cmdFail = CMD_FAIL_HTLC(add.id, Right(UnknownNextPeer), commit = true)
      commandBuffer ! CommandBuffer.CommandSend(add.channelId, add.id, cmdFail)

    case Status.Failure(addFailed: AddHtlcFailed) =>
      import addFailed.paymentHash
      addFailed.origin match {
        case Local(id, None) =>
          handleLocalPaymentAfterRestart(PaymentFailed(id, paymentHash, Nil))
        case Local(_, Some(sender)) =>
          sender ! Status.Failure(addFailed)
        case Relayed(originChannelId, originHtlcId, _, _) =>
          addFailed.originalCommand match {
            case Some(cmd) =>
              log.info(s"retrying htlc #$originHtlcId paymentHash=$paymentHash from channelId=$originChannelId")
              // NB: cmd.upstream.right is defined since this is a relayed payment
              self ! ForwardAdd(cmd.upstream.right.get, cmd.previousFailures :+ addFailed)
            case None =>
              val failure = translateError(addFailed)
              val cmdFail = CMD_FAIL_HTLC(originHtlcId, Right(failure), commit = true)
              log.info(s"rejecting htlc #$originHtlcId paymentHash=$paymentHash from channelId=$originChannelId reason=${cmdFail.reason}")
              commandBuffer ! CommandBuffer.CommandSend(originChannelId, originHtlcId, cmdFail)
          }
      }

    case ForwardFulfill(fulfill, to, add) =>
      to match {
        case Local(id, None) =>
          val feesPaid = 0.msat // fees are unknown since we lost the reference to the payment
          handleLocalPaymentAfterRestart(PaymentSent(id, add.paymentHash, fulfill.paymentPreimage, Seq(PaymentSent.PartialPayment(id, add.amountMsat, feesPaid, add.channelId, None))))
        case Local(_, Some(sender)) =>
          sender ! fulfill
        case Relayed(originChannelId, originHtlcId, amountIn, amountOut) =>
          val cmd = CMD_FULFILL_HTLC(originHtlcId, fulfill.paymentPreimage, commit = true)
          commandBuffer ! CommandBuffer.CommandSend(originChannelId, originHtlcId, cmd)
          context.system.eventStream.publish(PaymentRelayed(amountIn, amountOut, add.paymentHash, fromChannelId = originChannelId, toChannelId = fulfill.channelId))
      }

    case ForwardFail(fail, to, add) =>
      to match {
        case Local(id, None) =>
          handleLocalPaymentAfterRestart(PaymentFailed(id, add.paymentHash, Nil))
        case Local(_, Some(sender)) =>
          sender ! fail
        case Relayed(originChannelId, originHtlcId, _, _) =>
          val cmd = CMD_FAIL_HTLC(originHtlcId, Left(fail.reason), commit = true)
          commandBuffer ! CommandBuffer.CommandSend(originChannelId, originHtlcId, cmd)
      }

    case ForwardFailMalformed(fail, to, add) =>
      to match {
        case Local(id, None) =>
          handleLocalPaymentAfterRestart(PaymentFailed(id, add.paymentHash, Nil))
        case Local(_, Some(sender)) =>
          sender ! fail
        case Relayed(originChannelId, originHtlcId, _, _) =>
          val cmd = CMD_FAIL_MALFORMED_HTLC(originHtlcId, fail.onionHash, fail.failureCode, commit = true)
          commandBuffer ! CommandBuffer.CommandSend(originChannelId, originHtlcId, cmd)
      }

    case ack: CommandBuffer.CommandAck => commandBuffer forward ack

    case "ok" => () // ignoring responses from channels
  }

  /**
   * It may happen that we sent a payment and then re-started before the payment completed.
   * When we receive the HTLC fulfill/fail associated to that payment, the payment FSM that generated them doesn't exist
   * anymore so we need to reconcile the database.
   */
  def handleLocalPaymentAfterRestart(paymentResult: PaymentEvent): Unit = paymentResult match {
    case e: PaymentFailed =>
      nodeParams.db.payments.updateOutgoingPayment(e)
      // Since payments can be multi-part, we only emit the payment failed event once all child payments have failed.
      nodeParams.db.payments.getOutgoingPayment(e.id).foreach(p => {
        val payments = nodeParams.db.payments.listOutgoingPayments(p.parentId)
        if (payments.forall(_.status.isInstanceOf[OutgoingPaymentStatus.Failed])) {
          context.system.eventStream.publish(PaymentFailed(p.parentId, e.paymentHash, Nil))
        }
      })
    case e: PaymentSent =>
      nodeParams.db.payments.updateOutgoingPayment(e)
      // Since payments can be multi-part, we only emit the payment sent event once all child payments have settled.
      nodeParams.db.payments.getOutgoingPayment(e.id).foreach(p => {
        val payments = nodeParams.db.payments.listOutgoingPayments(p.parentId)
        if (!payments.exists(p => p.status == OutgoingPaymentStatus.Pending)) {
          val succeeded = payments.collect {
            case OutgoingPayment(id, _, _, _, amount, _, _, _, OutgoingPaymentStatus.Succeeded(_, feesPaid, _, completedAt)) =>
              PaymentSent.PartialPayment(id, amount, feesPaid, ByteVector32.Zeroes, None, completedAt)
          }
          context.system.eventStream.publish(PaymentSent(p.parentId, e.paymentHash, e.paymentPreimage, succeeded))
        }
      })
    case _ =>
  }

}

object Relayer extends Logging {
  def props(nodeParams: NodeParams, router: ActorRef, register: ActorRef, paymentHandler: ActorRef) = Props(classOf[Relayer], nodeParams, router, register, paymentHandler)

  case class OutgoingChannel(nextNodeId: PublicKey, channelUpdate: ChannelUpdate, commitments: Commitments)

  // @formatter:off
  sealed trait NextPayload
  case class FinalPayload(add: UpdateAddHtlc, payload: Onion.FinalPayload) extends NextPayload
  case class RelayPayload(add: UpdateAddHtlc, payload: Onion.RelayPayload, nextPacket: OnionRoutingPacket) extends NextPayload {
    val relayFeeMsat: MilliSatoshi = add.amountMsat - payload.amountToForward
    val expiryDelta: CltvExpiryDelta = add.cltvExpiry - payload.outgoingCltv
  }
  // TODO: @t-bast: will probably trigger that relay with multiple incoming UpdateAddHtlc (AMP)
  case class TrampolinePayload(add: UpdateAddHtlc, payload: Onion.FinalPayload, nextPayload: Onion.RelayTrampolinePayload, nextTrampolinePacket: OnionRoutingPacket) extends NextPayload
  // @formatter:on

  /**
   * Decrypt the onion of a received htlc, and find out if the payment is to be relayed, or if our node is the last one
   * in the route.
   *
   * @param add        incoming htlc
   * @param privateKey this node's private key
   * @return the payload for the next hop or an error.
   */
  def decryptPacket(add: UpdateAddHtlc, privateKey: PrivateKey, features: ByteVector): Either[FailureMessage, NextPayload] =
    Sphinx.PaymentPacket.peel(privateKey, add.paymentHash, add.onionRoutingPacket) match {
      case Right(p@Sphinx.DecryptedPacket(payload, nextPacket, _)) =>
        val codec = if (p.isLastPacket) OnionCodecs.finalPerHopPayloadCodec else OnionCodecs.relayPerHopPayloadCodec
        codec.decode(payload.bits) match {
          case Attempt.Successful(DecodeResult(_: Onion.TlvFormat, _)) if !Features.hasVariableLengthOnion(features) => Left(InvalidRealm)
          case Attempt.Successful(DecodeResult(perHopPayload, remainder)) =>
            if (remainder.nonEmpty) {
              logger.warn(s"${remainder.length} bits remaining after per-hop payload decoding: there might be an issue with the onion codec")
            }
            perHopPayload match {
              case finalPayload: Onion.FinalTlvPayload if finalPayload.records.get[TrampolineOnion].isDefined =>
                Sphinx.TrampolinePacket.peel(privateKey, add.paymentHash, finalPayload.records.get[TrampolineOnion].get.packet) match {
                  // TODO: @t-bast: not the right codec if we're the final trampoline node (and messy decrypt)
                  case Right(p@Sphinx.DecryptedPacket(payload, nextPacket, _)) => OnionCodecs.tlvPerHopPayloadCodec.decode(payload.bits) match {
                    case Attempt.Successful(DecodeResult(tlvRecords, _)) => Right(TrampolinePayload(add, finalPayload, Onion.RelayTrampolinePayload(tlvRecords), nextPacket))
                    case Attempt.Failure(_) => Left(InvalidOnionPayload(UInt64(0), 0))
                  }
                  case Left(badOnion) => Left(badOnion)
                }
              case finalPayload: Onion.FinalPayload => Right(FinalPayload(add, finalPayload))
              case relayPayload: Onion.RelayPayload => Right(RelayPayload(add, relayPayload, nextPacket))
            }
          case Attempt.Failure(e: OnionCodecs.MissingRequiredTlv) => Left(e.failureMessage)
          // Onion is correctly encrypted but the content of the per-hop payload couldn't be parsed.
          // It's hard to provide tag and offset information from scodec failures, so we currently don't do it.
          case Attempt.Failure(_) => Left(InvalidOnionPayload(UInt64(0), 0))
        }
      case Left(badOnion) => Left(badOnion)
    }

  def validateTrampoline(p: TrampolinePayload): Option[CMD_FAIL_HTLC] = {
    if (p.add.amountMsat < p.payload.amount || p.payload.amount <= p.nextPayload.amountToForward) {
      Some(CMD_FAIL_HTLC(p.add.id, Right(FinalIncorrectHtlcAmount(p.add.amountMsat)), commit = true))
    } else if (p.add.cltvExpiry != p.payload.expiry || p.payload.expiry <= p.nextPayload.outgoingCltv) {
      Some(CMD_FAIL_HTLC(p.add.id, Right(FinalIncorrectCltvExpiry(p.add.cltvExpiry)), commit = true))
    } else {
      None
    }
  }

  /**
   * Validate an incoming htlc when we are the last node.
   * Verifies that values inside the onion match the HTLC.
   *
   * @param p final payload
   * @return either:
   *         - a CMD_FAIL_HTLC to be sent back upstream
   *         - None if we should forward
   */
  def validateFinal(p: FinalPayload): Option[CMD_FAIL_HTLC] = {
    if (p.add.amountMsat < p.payload.amount) {
      Some(CMD_FAIL_HTLC(p.add.id, Right(FinalIncorrectHtlcAmount(p.add.amountMsat)), commit = true))
    } else if (p.add.cltvExpiry != p.payload.expiry) {
      Some(CMD_FAIL_HTLC(p.add.id, Right(FinalIncorrectCltvExpiry(p.add.cltvExpiry)), commit = true))
    } else {
      None
    }
  }

  // @formatter:off
  sealed trait RelayResult
  case class RelayFailure(cmdFail: CMD_FAIL_HTLC) extends RelayResult
  case class RelaySuccess(shortChannelId: ShortChannelId, cmdAdd: CMD_ADD_HTLC) extends RelayResult
  // @formatter:on

  /**
   * Handle an incoming htlc when we are a relaying node
   *
   * @param relayPayload payload
   * @return either:
   *         - a CMD_FAIL_HTLC to be sent back upstream
   *         - a CMD_ADD_HTLC to propagate downstream
   */
  def handleRelay(relayPayload: RelayPayload, channelUpdates: Map[ShortChannelId, OutgoingChannel], node2channels: mutable.Map[PublicKey, mutable.Set[ShortChannelId]] with mutable.MultiMap[PublicKey, ShortChannelId], previousFailures: Seq[AddHtlcFailed], chainHash: ByteVector32)(implicit log: LoggingAdapter): RelayResult = {
    import relayPayload._
    log.info(s"relaying htlc #${add.id} paymentHash={} from channelId={} to requestedShortChannelId={} previousAttempts={}", add.paymentHash, add.channelId, relayPayload.payload.outgoingChannelId, previousFailures.size)
    val alreadyTried = previousFailures.flatMap(_.channelUpdate).map(_.shortChannelId)
    selectPreferredChannel(relayPayload, channelUpdates, node2channels, alreadyTried)
      .flatMap(selectedShortChannelId => channelUpdates.get(selectedShortChannelId).map(_.channelUpdate)) match {
      case None if previousFailures.nonEmpty =>
        // no more channels to try
        val error = previousFailures
          // we return the error for the initially requested channel if it exists
          .find(_.channelUpdate.map(_.shortChannelId).contains(relayPayload.payload.outgoingChannelId))
          // otherwise we return the error for the first channel tried
          .getOrElse(previousFailures.head)
        RelayFailure(CMD_FAIL_HTLC(add.id, Right(translateError(error)), commit = true))
      case channelUpdate_opt =>
        relayOrFail(relayPayload, channelUpdate_opt, previousFailures)
    }
  }

  /**
   * Select a channel to the same node to relay the payment to, that has the lowest balance and is compatible in
   * terms of fees, expiry_delta, etc.
   *
   * If no suitable channel is found we default to the originally requested channel.
   */
  def selectPreferredChannel(relayPayload: RelayPayload, channelUpdates: Map[ShortChannelId, OutgoingChannel], node2channels: mutable.Map[PublicKey, mutable.Set[ShortChannelId]] with mutable.MultiMap[PublicKey, ShortChannelId], alreadyTried: Seq[ShortChannelId])(implicit log: LoggingAdapter): Option[ShortChannelId] = {
    import relayPayload.add
    val requestedShortChannelId = relayPayload.payload.outgoingChannelId
    log.debug(s"selecting next channel for htlc #${add.id} paymentHash={} from channelId={} to requestedShortChannelId={} previousAttempts={}", add.paymentHash, add.channelId, requestedShortChannelId, alreadyTried.size)
    // first we find out what is the next node
    val nextNodeId_opt = channelUpdates.get(requestedShortChannelId) match {
      case Some(OutgoingChannel(nextNodeId, _, _)) =>
        Some(nextNodeId)
      case None => None
    }
    nextNodeId_opt match {
      case Some(nextNodeId) =>
        log.debug(s"next hop for htlc #{} paymentHash={} is nodeId={}", add.id, add.paymentHash, nextNodeId)
        // then we retrieve all known channels to this node
        val allChannels = node2channels.getOrElse(nextNodeId, Set.empty[ShortChannelId])
        // we then filter out channels that we have already tried
        val candidateChannels = allChannels -- alreadyTried
        // and we filter keep the ones that are compatible with this payment (mainly fees, expiry delta)
        candidateChannels
          .map { shortChannelId =>
            val channelInfo_opt = channelUpdates.get(shortChannelId)
            val channelUpdate_opt = channelInfo_opt.map(_.channelUpdate)
            val relayResult = relayOrFail(relayPayload, channelUpdate_opt)
            log.debug(s"candidate channel for htlc #${add.id} paymentHash=${add.paymentHash}: shortChannelId={} balanceMsat={} channelUpdate={} relayResult={}", shortChannelId, channelInfo_opt.map(_.commitments.availableBalanceForSend).getOrElse(""), channelUpdate_opt.getOrElse(""), relayResult)
            (shortChannelId, channelInfo_opt, relayResult)
          }
          .collect { case (shortChannelId, Some(channelInfo), _: RelaySuccess) => (shortChannelId, channelInfo.commitments.availableBalanceForSend) }
          .filter(_._2 > relayPayload.payload.amountToForward) // we only keep channels that have enough balance to handle this payment
          .toList // needed for ordering
          .sortBy(_._2) // we want to use the channel with the lowest available balance that can process the payment
          .headOption match {
          case Some((preferredShortChannelId, availableBalanceMsat)) if preferredShortChannelId != requestedShortChannelId =>
            log.info("replacing requestedShortChannelId={} by preferredShortChannelId={} with availableBalanceMsat={}", requestedShortChannelId, preferredShortChannelId, availableBalanceMsat)
            Some(preferredShortChannelId)
          case Some(_) =>
            // the requested short_channel_id is already our preferred channel
            Some(requestedShortChannelId)
          case None if !alreadyTried.contains(requestedShortChannelId) =>
            // no channel seem to work for this payment, we keep the requested channel id
            Some(requestedShortChannelId)
          case None =>
            // no channel seem to work for this payment and we have already tried the requested channel id: we give up
            None
        }
      case _ => Some(requestedShortChannelId) // we don't have a channel_update for this short_channel_id
    }
  }

  /**
   * This helper method will tell us if it is not even worth attempting to relay the payment to our local outgoing
   * channel, because some parameters don't match with our settings for that channel. In that case we directly fail the
   * htlc.
   */
  def relayOrFail(relayPayload: RelayPayload, channelUpdate_opt: Option[ChannelUpdate], previousFailures: Seq[AddHtlcFailed] = Seq.empty)(implicit log: LoggingAdapter): RelayResult = {
    import relayPayload._
    channelUpdate_opt match {
      case None =>
        RelayFailure(CMD_FAIL_HTLC(add.id, Right(UnknownNextPeer), commit = true))
      case Some(channelUpdate) if !Announcements.isEnabled(channelUpdate.channelFlags) =>
        RelayFailure(CMD_FAIL_HTLC(add.id, Right(ChannelDisabled(channelUpdate.messageFlags, channelUpdate.channelFlags, channelUpdate)), commit = true))
      case Some(channelUpdate) if payload.amountToForward < channelUpdate.htlcMinimumMsat =>
        RelayFailure(CMD_FAIL_HTLC(add.id, Right(AmountBelowMinimum(payload.amountToForward, channelUpdate)), commit = true))
      case Some(channelUpdate) if relayPayload.expiryDelta != channelUpdate.cltvExpiryDelta =>
        RelayFailure(CMD_FAIL_HTLC(add.id, Right(IncorrectCltvExpiry(payload.outgoingCltv, channelUpdate)), commit = true))
      case Some(channelUpdate) if relayPayload.relayFeeMsat < nodeFee(channelUpdate.feeBaseMsat, channelUpdate.feeProportionalMillionths, payload.amountToForward) =>
        RelayFailure(CMD_FAIL_HTLC(add.id, Right(FeeInsufficient(add.amountMsat, channelUpdate)), commit = true))
      case Some(channelUpdate) =>
        RelaySuccess(channelUpdate.shortChannelId, CMD_ADD_HTLC(payload.amountToForward, add.paymentHash, payload.outgoingCltv, nextPacket, upstream = Right(add), commit = true, previousFailures = previousFailures))
    }
  }

  /**
   * This helper method translates relaying errors (returned by the downstream outgoing channel) to BOLT 4 standard
   * errors that we should return upstream.
   */
  private def translateError(failure: AddHtlcFailed): FailureMessage = {
    val error = failure.t
    val channelUpdate_opt = failure.channelUpdate
    (error, channelUpdate_opt) match {
      case (_: ExpiryTooSmall, Some(channelUpdate)) => ExpiryTooSoon(channelUpdate)
      case (_: ExpiryTooBig, _) => ExpiryTooFar
      case (_: InsufficientFunds, Some(channelUpdate)) => TemporaryChannelFailure(channelUpdate)
      case (_: TooManyAcceptedHtlcs, Some(channelUpdate)) => TemporaryChannelFailure(channelUpdate)
      case (_: ChannelUnavailable, Some(channelUpdate)) if !Announcements.isEnabled(channelUpdate.channelFlags) => ChannelDisabled(channelUpdate.messageFlags, channelUpdate.channelFlags, channelUpdate)
      case (_: ChannelUnavailable, None) => PermanentChannelFailure
      case (_: HtlcTimedout, _) => PermanentChannelFailure
      case _ => TemporaryNodeFailure
    }
  }

}
