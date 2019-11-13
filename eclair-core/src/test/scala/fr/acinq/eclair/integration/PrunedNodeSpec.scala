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

package fr.acinq.eclair.integration

import java.io.{File, PrintWriter}
import java.util.{Properties, UUID}

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import com.google.common.net.HostAndPort
import com.typesafe.config.{Config, ConfigFactory}
import fr.acinq.bitcoin.Crypto.{PrivateKey, PublicKey}
import fr.acinq.bitcoin.{Base58, Base58Check, Bech32, Block, ByteVector32, Crypto, OP_0, OP_CHECKSIG, OP_DUP, OP_EQUAL, OP_EQUALVERIFY, OP_HASH160, OP_PUSHDATA, Satoshi, Script, ScriptFlags, Transaction}
import fr.acinq.eclair.blockchain.bitcoind.BitcoindService
import fr.acinq.eclair.blockchain.bitcoind.rpc.ExtendedBitcoinClient
import fr.acinq.eclair.blockchain.{Watch, WatchConfirmed}
import fr.acinq.eclair.channel.Channel.{BroadcastChannelUpdate, PeriodicRefresh}
import fr.acinq.eclair.channel.Register.{Forward, ForwardShortId}
import fr.acinq.eclair.channel._
import fr.acinq.eclair.crypto.Sphinx.DecryptedFailurePacket
import fr.acinq.eclair.io.Peer
import fr.acinq.eclair.io.Peer.{Disconnect, PeerRoutingMessage}
import fr.acinq.eclair.payment.PaymentInitiator.SendPaymentRequest
import fr.acinq.eclair.payment.PaymentLifecycle.{State => _, _}
import fr.acinq.eclair.payment._
import fr.acinq.eclair.router.Graph.WeightRatios
import fr.acinq.eclair.router.Router.ROUTE_MAX_LENGTH
import fr.acinq.eclair.router.{Announcements, AnnouncementsBatchValidationSpec, PublicChannel, RouteParams}
import fr.acinq.eclair.transactions.Transactions
import fr.acinq.eclair.transactions.Transactions.{HtlcSuccessTx, HtlcTimeoutTx}
import fr.acinq.eclair.wire._
import fr.acinq.eclair.{CltvExpiryDelta, Kit, LongToBtcAmount, MilliSatoshi, Setup, ShortChannelId, TestConstants, randomBytes32}
import grizzled.slf4j.Logging
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import scodec.bits.ByteVector

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Created by fjahr on 13/11/2019.
 * This is a copy of IntegrationSpec with some stuff removed but still lots more left to be cleaned up.
 */

class PrunedNodeSpec extends TestKit(ActorSystem("test")) with BitcoindService with FunSuiteLike with BeforeAndAfterAll with Logging {

  var nodes: Map[String, Kit] = Map()

  // we override the default because these test were designed to use cost-optimized routes
  val integrationTestRouteParams = Some(RouteParams(
    randomize = false,
    maxFeeBase = MilliSatoshi(Long.MaxValue),
    maxFeePct = Double.MaxValue,
    routeMaxCltv = CltvExpiryDelta(Int.MaxValue),
    routeMaxLength = ROUTE_MAX_LENGTH,
    ratios = Some(WeightRatios(
      cltvDeltaFactor = 0.1,
      ageFactor = 0,
      capacityFactor = 0
    ))
  ))

  val commonConfig = ConfigFactory.parseMap(Map(
    "eclair.chain" -> "regtest",
    "eclair.server.public-ips.1" -> "127.0.0.1",
    "eclair.bitcoind.port" -> bitcoindPort,
    "eclair.bitcoind.rpcport" -> bitcoindRpcPort,
    "eclair.bitcoind.zmqblock" -> s"tcp://127.0.0.1:$bitcoindZmqBlockPort",
    "eclair.bitcoind.zmqtx" -> s"tcp://127.0.0.1:$bitcoindZmqTxPort",
    "eclair.mindepth-blocks" -> 2,
    "eclair.max-htlc-value-in-flight-msat" -> 100000000000L,
    "eclair.router.broadcast-interval" -> "2 second",
    "eclair.auto-reconnect" -> false,
    "eclair.to-remote-delay-blocks" -> 144))

  override def beforeAll(): Unit = {
    startBitcoind(prune = 1)
  }

  override def afterAll(): Unit = {
    // gracefully stopping bitcoin will make it store its state cleanly to disk, which is good for later debugging
    logger.info(s"stopping bitcoind")
    stopBitcoind()
    nodes.foreach {
      case (name, setup) =>
        logger.info(s"stopping node $name")
        setup.system.terminate()
    }
  }

  test("wait bitcoind ready") {
    val sender = TestProbe()
    logger.info(s"waiting for bitcoind to initialize...")
    awaitCond({
      sender.send(bitcoincli, BitcoinReq("getnetworkinfo"))
      sender.receiveOne(5 second).isInstanceOf[JValue]
    }, max = 30 seconds, interval = 500 millis)
    logger.info(s"generating initial blocks...")
    generateBlocks(bitcoincli, 150, timeout = 30 seconds)
  }

  def instantiateEclairNode(name: String, config: Config) = {
    val datadir = new File(INTEGRATION_TMP_DIR, s"datadir-eclair-$name")
    datadir.mkdirs()
    new PrintWriter(new File(datadir, "eclair.conf")) {
      write(config.root().render())
      close
    }
    implicit val system = ActorSystem(s"system-$name")
    val setup = new Setup(datadir)
    val kit = Await.result(setup.bootstrap, 10 seconds)
    nodes = nodes + (name -> kit)
  }

  def javaProps(props: Seq[(String, String)]) = {
    val properties = new Properties()
    props.foreach(p => properties.setProperty(p._1, p._2))
    properties
  }

  test("starting eclair nodes") {
    import collection.JavaConversions._
    instantiateEclairNode("A", ConfigFactory.parseMap(Map("eclair.node-alias" -> "A", "eclair.expiry-delta-blocks" -> 130, "eclair.server.port" -> 29730, "eclair.api.port" -> 28080, "eclair.payment-handler" -> "noop")).withFallback(commonConfig))
    instantiateEclairNode("B", ConfigFactory.parseMap(Map("eclair.node-alias" -> "B", "eclair.expiry-delta-blocks" -> 131, "eclair.server.port" -> 29731, "eclair.api.port" -> 28081, "eclair.payment-handler" -> "noop")).withFallback(commonConfig))
    // instantiateEclairNode("C", ConfigFactory.parseMap(Map("eclair.node-alias" -> "C", "eclair.expiry-delta-blocks" -> 132, "eclair.server.port" -> 29732, "eclair.api.port" -> 28082)).withFallback(commonConfig))

    // by default A has a normal payment handler, but this can be overriden in tests
    val paymentHandlerA = nodes("A").system.actorOf(LocalPaymentHandler.props(nodes("A").nodeParams))
    nodes("A").paymentHandler ! paymentHandlerA
    val paymentHandlerB = nodes("B").system.actorOf(LocalPaymentHandler.props(nodes("B").nodeParams))
    nodes("B").paymentHandler ! paymentHandlerB
  }

  def connect(node1: Kit, node2: Kit) = {
    val sender = TestProbe()
    val address = node2.nodeParams.publicAddresses.head
    sender.send(node1.switchboard, Peer.Connect(
      nodeId = node2.nodeParams.nodeId,
      address_opt = Some(HostAndPort.fromParts(address.socketAddress.getHostString, address.socketAddress.getPort))
    ))
    sender.expectMsgAnyOf(10 seconds, "connected", "already connected")
  }

  def openChannel(node1: Kit, node2: Kit, fundingSatoshis: Satoshi, pushMsat: MilliSatoshi) = {
    val sender = TestProbe()
    sender.send(node1.switchboard, Peer.OpenChannel(
      remoteNodeId = node2.nodeParams.nodeId,
      fundingSatoshis = fundingSatoshis,
      pushMsat = pushMsat,
      fundingTxFeeratePerKw_opt = None,
      channelFlags = None,
      timeout_opt = None))
    assert(sender.expectMsgType[String](10 seconds).startsWith("created channel"))
  }

  def connectWithChannel(node1: Kit, node2: Kit, fundingSatoshis: Satoshi, pushMsat: MilliSatoshi) = {
    connect(node1: Kit, node2: Kit)
    openChannel(node1: Kit, node2: Kit, fundingSatoshis: Satoshi, pushMsat: MilliSatoshi)
  }

  test("connect nodes") {
    // A---B---C

    val sender = TestProbe()
    val eventListener = TestProbe()
    nodes.values.foreach(_.system.eventStream.subscribe(eventListener.ref, classOf[ChannelStateChanged]))

    connectWithChannel(nodes("A"), nodes("B"), 11000000 sat, 0 msat)
    // connect(nodes("B"), nodes("C"))

    val numberOfChannels = 1
    val channelEndpointsCount = 2 * numberOfChannels

    // we make sure all channels have set up their WatchConfirmed for the funding tx
    awaitCond({
      val watches = nodes.values.foldLeft(Set.empty[Watch]) {
        case (watches, setup) =>
          sender.send(setup.watcher, 'watches)
          watches ++ sender.expectMsgType[Set[Watch]]
      }
      watches.count(_.isInstanceOf[WatchConfirmed]) == channelEndpointsCount
    }, max = 20 seconds, interval = 1 second)

    // confirming the funding tx
    generateBlocks(bitcoincli, 2)

    within(60 seconds) {
      var count = 0
      while (count < channelEndpointsCount) {
        if (eventListener.expectMsgType[ChannelStateChanged](30 seconds).currentState == NORMAL) count = count + 1
      }
    }
  }

  def awaitAnnouncements(subset: Map[String, Kit], nodes: Int, channels: Int, updates: Int) = {
    val sender = TestProbe()
    subset.foreach {
      case (_, setup) =>
        awaitCond({
          sender.send(setup.router, 'nodes)
          sender.expectMsgType[Iterable[NodeAnnouncement]](20 seconds).size == nodes
        }, max = 60 seconds, interval = 1 second)
        awaitCond({
          sender.send(setup.router, 'channels)
          sender.expectMsgType[Iterable[ChannelAnnouncement]](20 seconds).size == channels
        }, max = 60 seconds, interval = 1 second)
        awaitCond({
          sender.send(setup.router, 'updates)
          sender.expectMsgType[Iterable[ChannelUpdate]](20 seconds).size == updates
        }, max = 60 seconds, interval = 1 second)
    }
  }

  test("wait for network announcements") {
    val sender = TestProbe()
    // generating more blocks so that all funding txes are buried under at least 6 blocks
    generateBlocks(bitcoincli, 4)
    // A requires private channels, as a consequence:
    // - only A and B know about channel A-B (and there is no channel_announcement)
    // - A is not announced (no node_announcement)
    // awaitAnnouncements(nodes.filterKeys(key => List("A", "B").contains(key)), 2, 1, 1)
    // awaitAnnouncements(nodes.filterKeys(key => !List("A", "B").contains(key)), 3, 1, 2)
  }

  /**
   * We currently use p2pkh script Helpers.getFinalScriptPubKey
   */
  def scriptPubKeyToAddress(scriptPubKey: ByteVector) = Script.parse(scriptPubKey) match {
    case OP_DUP :: OP_HASH160 :: OP_PUSHDATA(pubKeyHash, _) :: OP_EQUALVERIFY :: OP_CHECKSIG :: Nil =>
      Base58Check.encode(Base58.Prefix.PubkeyAddressTestnet, pubKeyHash)
    case OP_HASH160 :: OP_PUSHDATA(scriptHash, _) :: OP_EQUAL :: Nil =>
      Base58Check.encode(Base58.Prefix.ScriptAddressTestnet, scriptHash)
    case OP_0 :: OP_PUSHDATA(pubKeyHash, _) :: Nil if pubKeyHash.length == 20 => Bech32.encodeWitnessAddress("bcrt", 0, pubKeyHash)
    case OP_0 :: OP_PUSHDATA(scriptHash, _) :: Nil if scriptHash.length == 32 => Bech32.encodeWitnessAddress("bcrt", 0, scriptHash)
    case _ => ???
  }

  def getBlockCount: Long = {
    // we make sure that all nodes have the same value
    awaitCond(nodes.values.map(_.nodeParams.currentBlockHeight).toSet.size == 1, max = 1 minute, interval = 1 second)
    // and we return it (NB: it could be a different value at this point
    nodes.values.head.nodeParams.currentBlockHeight
  }

  // To work this test requires a configured bitcoind with small block storage size (default is 128MB).
  // With a normal bitcoind as the backend none of these empty block will ever get pruned because
  // they don't fill up a single block storage file and so they will not be removed and bitcoind
  // will respond to the pruneblockchain RPC call with just 0 as the new prune height (as it was not
  // changed).
  test("punish a node that has published a revoked commit tx that was in a pruned block") {
    val sender = TestProbe()
    // we subscribe to A's channel state transitions
    val stateListener = TestProbe()
    nodes("A").system.eventStream.subscribe(stateListener.ref, classOf[ChannelStateChanged])
    // we use this to get commitments
    val sigListener = TestProbe()
    nodes("B").system.eventStream.subscribe(sigListener.ref, classOf[ChannelSignatureReceived])
    // we use this to control when to fulfill htlcs, setup is as follow : noop-handler ---> forward-handler ---> payment-handler
    val forwardHandlerA = TestProbe()
    nodes("A").paymentHandler ! forwardHandlerA.ref
    val forwardHandlerB = TestProbe()
    nodes("B").paymentHandler ! forwardHandlerB.ref
    // this is the actual payment handler that we will forward requests to
    val paymentHandlerA = nodes("A").system.actorOf(LocalPaymentHandler.props(nodes("A").nodeParams))
    val paymentHandlerB = nodes("B").system.actorOf(LocalPaymentHandler.props(nodes("B").nodeParams))
    // first we make sure we are in sync with current blockchain height
    val currentBlockCount = getBlockCount
    awaitCond(currentBlockCount == getBlockCount, max = 20 seconds, interval = 1 second)
    // Generate lots of blocks so we can prune
    generateBlocks(bitcoincli, 1000)
    awaitCond(currentBlockCount + 1000 == getBlockCount, max = 20 seconds, interval = 1 second)
    // prune blocks againg so funding transactions have been pruned
    pruneBlockchain(bitcoincli, 500)
    // first we send 3 mBTC to B so that it has a balance
    val amountMsat = 300000000.msat
    sender.send(paymentHandlerB, ReceivePayment(Some(amountMsat), "1 coffee"))
    val pr = sender.expectMsgType[PaymentRequest]
    val sendReq = SendPaymentRequest(300000000 msat, pr.paymentHash, pr.nodeId, routeParams = integrationTestRouteParams, maxAttempts = 1)
    sender.send(nodes("A").paymentInitiator, sendReq)
    val paymentId = sender.expectMsgType[UUID]
    // we forward the htlc to the payment handler
    forwardHandlerB.expectMsgType[UpdateAddHtlc]
    forwardHandlerB.forward(paymentHandlerB)
    sigListener.expectMsgType[ChannelSignatureReceived]
    sigListener.expectMsgType[ChannelSignatureReceived]
    sender.expectMsgType[PaymentSent].id === paymentId

    // we now send a few htlcs A->B and B->A in order to obtain a commitments with multiple htlcs
    def send(amountMsat: MilliSatoshi, paymentHandler: ActorRef, paymentInitiator: ActorRef) = {
      sender.send(paymentHandler, ReceivePayment(Some(amountMsat), "1 coffee"))
      val pr = sender.expectMsgType[PaymentRequest]
      val sendReq = SendPaymentRequest(amountMsat, pr.paymentHash, pr.nodeId, routeParams = integrationTestRouteParams, maxAttempts = 1)
      sender.send(paymentInitiator, sendReq)
      sender.expectMsgType[UUID]
    }

    val buffer = TestProbe()
    send(100000000 msat, paymentHandlerB, nodes("A").paymentInitiator) // will be left pending
    forwardHandlerB.expectMsgType[UpdateAddHtlc]
    forwardHandlerB.forward(buffer.ref)
    sigListener.expectMsgType[ChannelSignatureReceived]
    send(110000000 msat, paymentHandlerB, nodes("A").paymentInitiator) // will be left pending
    forwardHandlerB.expectMsgType[UpdateAddHtlc]
    forwardHandlerB.forward(buffer.ref)
    sigListener.expectMsgType[ChannelSignatureReceived]
    send(120000000 msat, paymentHandlerA, nodes("B").paymentInitiator)
    forwardHandlerA.expectMsgType[UpdateAddHtlc]
    forwardHandlerA.forward(buffer.ref)
    sigListener.expectMsgType[ChannelSignatureReceived]
    send(5000000 msat, paymentHandlerA, nodes("B").paymentInitiator)
    forwardHandlerA.expectMsgType[UpdateAddHtlc]
    forwardHandlerA.forward(buffer.ref)
    val commitmentsB = sigListener.expectMsgType[ChannelSignatureReceived].commitments
    sigListener.expectNoMsg(1 second)
    // in this commitment, both parties should have a main output, and there are four pending htlcs
    val localCommitB = commitmentsB.localCommit.publishableTxs
    assert(localCommitB.commitTx.tx.txOut.size === 5)
    val htlcTimeoutTxs = localCommitB.htlcTxsAndSigs.collect { case h@HtlcTxAndSigs(_: HtlcTimeoutTx, _, _) => h }
    val htlcSuccessTxs = localCommitB.htlcTxsAndSigs.collect { case h@HtlcTxAndSigs(_: HtlcSuccessTx, _, _) => h }
    assert(htlcTimeoutTxs.size === 1)
    assert(htlcSuccessTxs.size === 2)
    // we fulfill htlcs to get the preimagse
    buffer.expectMsgType[UpdateAddHtlc]
    buffer.forward(paymentHandlerB)
    sigListener.expectMsgType[ChannelSignatureReceived]
    val preimage1 = sender.expectMsgType[PaymentSent].paymentPreimage
    buffer.expectMsgType[UpdateAddHtlc]
    buffer.forward(paymentHandlerB)
    sigListener.expectMsgType[ChannelSignatureReceived]
    sender.expectMsgType[PaymentSent].paymentPreimage
    buffer.expectMsgType[UpdateAddHtlc]
    buffer.forward(paymentHandlerA)
    sigListener.expectMsgType[ChannelSignatureReceived]
    sender.expectMsgType[PaymentSent].paymentPreimage
    buffer.expectMsgType[UpdateAddHtlc]
    buffer.forward(paymentHandlerA)
    sigListener.expectMsgType[ChannelSignatureReceived]
    sender.expectMsgType[PaymentSent].paymentPreimage
    // this also allows us to get the channel id
    val channelId = commitmentsB.channelId
    // we also retrieve A's default final address
    sender.send(nodes("A").register, Forward(channelId, CMD_GETSTATEDATA))
    val finalAddressA = scriptPubKeyToAddress(sender.expectMsgType[DATA_NORMAL].commitments.localParams.defaultFinalScriptPubKey)
    // and we retrieve transactions already received so that we don't take them into account when evaluating the outcome of this test
    sender.send(bitcoincli, BitcoinReq("listreceivedbyaddress", 0))
    val res = sender.expectMsgType[JValue](10 seconds)
    val previouslyReceivedByA = res.filter(_ \ "address" == JString(finalAddressA)).flatMap(_ \ "txids" \\ classOf[JString])
    // B will publish the commitment above, which is now revoked
    val revokedCommitTx = localCommitB.commitTx.tx
    val htlcSuccess = Transactions.addSigs(htlcSuccessTxs.head.txinfo.asInstanceOf[HtlcSuccessTx], htlcSuccessTxs.head.localSig, htlcSuccessTxs.head.remoteSig, preimage1).tx
    val htlcTimeout = Transactions.addSigs(htlcTimeoutTxs.head.txinfo.asInstanceOf[HtlcTimeoutTx], htlcTimeoutTxs.head.localSig, htlcTimeoutTxs.head.remoteSig).tx
    Transaction.correctlySpends(htlcSuccess, Seq(revokedCommitTx), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
    Transaction.correctlySpends(htlcTimeout, Seq(revokedCommitTx), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
    // we then generate blocks to make the htlc timeout (nothing will happen in the channel because all of them have already been fulfilled)
    generateBlocks(bitcoincli, 20)
    // then we publish B's revoked transactions
    sender.send(bitcoincli, BitcoinReq("sendrawtransaction", revokedCommitTx.toString()))
    sender.expectMsgType[JValue](10000 seconds)
    sender.send(bitcoincli, BitcoinReq("sendrawtransaction", htlcSuccess.toString()))
    sender.expectMsgType[JValue](10000 seconds)
    sender.send(bitcoincli, BitcoinReq("sendrawtransaction", htlcTimeout.toString()))
    sender.expectMsgType[JValue](10000 seconds)
    // at this point A should have 3 recv transactions: its previous main output, and B's main and htlc output (taken as punishment)
    awaitCond({
      sender.send(bitcoincli, BitcoinReq("listreceivedbyaddress", 0))
      val res = sender.expectMsgType[JValue](10 seconds)
      val receivedByA = res.filter(_ \ "address" == JString(finalAddressA)).flatMap(_ \ "txids" \\ classOf[JString])
      (receivedByA diff previouslyReceivedByA).size == 5
    }, max = 30 seconds, interval = 1 second)
    // we generate blocks to make tx confirm
    generateBlocks(bitcoincli, 2)
    // and we wait for A'channel to close
    awaitCond(stateListener.expectMsgType[ChannelStateChanged].currentState == CLOSED, max = 30 seconds)
    // this will remove the channel
    awaitAnnouncements(nodes.filterKeys(_ == "A"), 5, 7, 16)
  }

  // Other tests that could be implemented here:
  //
  test("one of our channels (funder or fundee) is closed cooperatively but block with funding tx was pruned") {}
  // should close channel normally
  test("one of our channels (funder or fundee) is closed maliciously but block with funding tx was pruned") {}
  // should be able to punish the malicious peer
  test("routing channel announcements/gossip where block was pruned") {}
  // we should gracefully ignore that announcement
  test("routing channel closing announcement where block was pruned") {}
  // we should just remove the channel from the routing table as normal
  test("open channel message where input of funding tx was pruned") {}
  // should be ok to proceed since if something was wrong here the funding tx should not be mined
  test("eclair node comes back online after the last block it has seen was already pruned") {}
  // should be able to launch
}
