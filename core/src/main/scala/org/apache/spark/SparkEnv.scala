/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.File
import java.net.Socket

import scala.collection.mutable
import scala.util.Properties

import akka.actor.ActorSystem
import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.{RpcEndpointRef, RpcEndpoint, RpcEnv}
import org.apache.spark.rpc.akka.AkkaRpcEnv
import org.apache.spark.scheduler.{OutputCommitCoordinator, LiveListenerBus}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage._
import org.apache.spark.util.{AkkaUtils, RpcUtils, Utils}

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 *       in a future release.
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    _actorSystem: ActorSystem, // TODO Remove actorSystem
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheManager: CacheManager,                         //存储中间的计算结果
    val mapOutputTracker: MapOutputTracker,                 //缓存mapstatus信息
    val shuffleManager: ShuffleManager,                     //路由维护表
    val broadcastManager: BroadcastManager,                 //广播
    val blockTransferService: BlockTransferService,
    val blockManager: BlockManager,                         //块管理
    val securityManager: SecurityManager,                   //安全管理
    val sparkFilesDir: String,                              //文件存储目录
    val metricsSystem: MetricsSystem,                       //测量
    val memoryManager: MemoryManager,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf                                     //配置文件
               ) extends Logging {

  // TODO Remove actorSystem
  @deprecated("Actor system is no longer supported as of 1.4.0", "1.4.0")
  val actorSystem: ActorSystem = _actorSystem

  private[spark] var isStopped = false
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  private var driverTmpDirToDelete: Option[String] = None

  private[spark] def stop() {

    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      mapOutputTracker.stop()
      shuffleManager.stop()
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      if (!rpcEnv.isInstanceOf[AkkaRpcEnv]) {
        actorSystem.shutdown()
      }
      rpcEnv.shutdown()

      // Unfortunately Akka's awaitTermination doesn't actually wait for the Netty server to shut
      // down, but let's call it anyway in case it gets fixed in a later release
      // UPDATE: In Akka 2.1.x, this hangs if there are remote actors, so we can't call it.
      // actorSystem.awaitTermination()

      // Note that blockTransferService is stopped by BlockManager since it is started by it.

      // If we only stop sc, but the driver process still run as a services then we need to delete
      // the tmp dir, if not, it will create too many tmp dirs.
      // We only need to delete the tmp dir create by driver, because sparkFilesDir is point to the
      // current working dir in executor which we do not need to delete.
      driverTmpDirToDelete match {
        case Some(path) => {
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(s"Exception while deleting Spark temp dir: $path", e)
          }
        }
        case None => // We just need to delete tmp dir created by driver, so do nothing on executor
      }
    }
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark]
  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverActorSystemName = "sparkDriver"
  private[spark] val executorActorSystemName = "sparkExecutor"

  def set(e: SparkEnv) {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  /**
   * Returns the ThreadLocal SparkEnv.
   */
  @deprecated("Use SparkEnv.get instead", "1.2.0")
  def getThreadLocal: SparkEnv = {
    env
  }

  /**
   * Create a SparkEnv for the driver.
   */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains("spark.driver.host"), "spark.driver.host is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    val hostname = conf.get("spark.driver.host")
    val port = conf.get("spark.driver.port").toInt
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      hostname,
      port,
      isDriver = true,
      isLocal = isLocal,
      numUsableCores = numCores,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
   * Create a SparkEnv for an executor.
   * In coarse-grained mode, the executor provides an actor system that is already instantiated.
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      numCores: Int,
      isLocal: Boolean): SparkEnv = {
    val env = create(
      conf,
      executorId,
      hostname,
      port,
      isDriver = false,
      isLocal = isLocal,
      numUsableCores = numCores
    )
    SparkEnv.set(env)
    env
  }

  /**
    * Helper method to create a SparkEnv for a driver or an executor.
    *
    * 参考：http://blog.csdn.net/dwb1015/article/details/52467490
    *
    * SparkEnv的构造步骤如下：
    * 1. 创建安全管理器SecurityManager；
    * 2. 创建RpcEnv；
    * 3. 创建基于Akka的分布式消息系统ActorSystem（注意：Spark 1.4.0之后已经废弃了）
    * 4. 创建Map任务输出跟踪器MapOutputTracker；
    * 5. 创建ShuffleManager；
    * 6. 内存管理器MemoryManager；
    * 7. 创建块传输服务NettyBlockTransferService；
    * 8. 创建BlockManagerMaster；
    * 9. 创建块管理器BlockManager；
    * 10. 创建广播管理器BroadcastManager；
    * 11. 创建缓存管理器CacheManager；
    * 12. 创建测量系统MetricsSystem；
    * 13. 创建OutputCommitCoordinator；
    * 14. 创建SparkEnv
   */
  private def create(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      isDriver: Boolean,
      isLocal: Boolean,
      numUsableCores: Int,
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    // Listener bus is only used on the driver
    // 是否是Driver，如果是driver必须有listenerBus
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }

    // 创建安全管理器
    // SecurityManager主要对权限、账号进行设置，如果使用Hadoop YARN作为集群管理器，则需要使用证书生成secret key登录，最后给当前系统设置默认的口令认证实例，此实例采用的是匿名内部类
    val securityManager = new SecurityManager(conf)

    // Create the ActorSystem for Akka and get the port it binds to.
    /**
      * 创建rpcEnv
      *
      * Spark1.6推出的RpcEnv、RpcEndPoint、RpcEndpointRef为核心的新型架构下的RPC通信方式，在底层封装了Akka和Netty，为未来扩充更多的通信系统提供了可能。
      * RpcEnv是RPC的环境，所有的RpcEndpoint都需要注册到RpcEnv实例对象中，管理着这些注册的RpcEndpoint的生命周期：
      *
      * - 根据name或者uri注册RpcEndpoint；
      * - 管理各种消息的处理；
      * - 停止RpcEndpoint
      */
    val actorSystemName = if (isDriver) driverActorSystemName else executorActorSystemName
    val rpcEnv = RpcEnv.create(actorSystemName, hostname, port, conf, securityManager,
      clientMode = !isDriver)
    // 创建基于Akka的分布式消息系统ActorSystem
    val actorSystem: ActorSystem =
      if (rpcEnv.isInstanceOf[AkkaRpcEnv]) {
        rpcEnv.asInstanceOf[AkkaRpcEnv].actorSystem
      } else {
        val actorSystemPort =
          if (port == 0 || rpcEnv.address == null) {
            port
          } else {
            rpcEnv.address.port + 1
          }
        // Create a ActorSystem for legacy codes
        AkkaUtils.createActorSystem(
          actorSystemName + "ActorSystem",
          hostname,
          actorSystemPort,
          conf,
          securityManager
        )._1
      }

    // Figure out which port Akka actually bound to in case the original port is 0 or occupied.
    // In the non-driver case, the RPC env's address may be null since it may not be listening
    // for incoming connections.
    if (isDriver) {
      conf.set("spark.driver.port", rpcEnv.address.port.toString)
    } else if (rpcEnv.address != null) {
      conf.set("spark.executor.port", rpcEnv.address.port.toString)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }

    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")

    val closureSerializer = instantiateClassFromConf[Serializer](
      "spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer")

    def registerOrLookupEndpoint(
        name: String, endpointCreator: => RpcEndpoint):
      RpcEndpointRef = {
      if (isDriver) {
        logInfo("Registering " + name)
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }


    /**
    * 创建Map任务输出跟踪器MapOutputTracker
      *
    * MapOutputTrack 用于跟踪Map阶段任务的输出状态，此状态便于Reduce阶段任务获取地址及中间结果。
    * 每个Map任务或者Reduce任务都会有其唯一的标识，分别为mapId 和 reduceId。
    * 每个Reduce任务的输入可能是多个Map任务的输出，Reduce会到各个Map任务的所在节点上拉取Block，这一过程叫做Shuffle。
    * 每个Shuffle过程都有唯一的表示shuffleId。
    *
    * MapOutputTracker 有两个子类：MapOutputTrackerMaster（for driver） 和 MapOutputTrackerWorker（for executors）；因为它们使用了不同的HashMap来存储元数据。
    */
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerActor after initialization as MapOutputTrackerActor
    // requires the MapOutputTracker itself
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))


    // Let the user specify short names for shuffle managers
    /**
      * 创建ShuffleManager
      *
      * ShuffleManager负责管理本地及远程的Block数据的shuffle操作。
      * ShuffleManager默认通过反射方式生成的SortShuffleManager的实例。
      * 默认使用的是sort模式的SortShuffleManager，当然也可以通过修改属性spark.shuffle.manager为hash来显式控制使用HashShuffleManager。
      * 在Spark1.6增加了tungsten-sort，为了更好的实现shuffle。tungsten应该在spark2.0做的更加完善。
      */
    val shortShuffleMgrNames = Map(
      "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
      "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager",
      "tungsten-sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager")
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

    /**
      * 创建管理器
      *
      * 增加了一个新的内存管理模型：UnifiedMemoryManager。
      * 该模型可以使得execution部分和storage部分的内存不像之前的（StaticMemoryManager）由比例参数限定住，而是两者可以互相借用空闲的内存。
      */
    val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)
    val memoryManager: MemoryManager =
      if (useLegacyMemoryManager) {
        new StaticMemoryManager(conf, numUsableCores)
      } else {
        UnifiedMemoryManager(conf, numUsableCores) //默认使用这个
      }

    /**
      * 创建块传输服务
      *
      * NettyBlockTransferService使用Netty提供的异步事件驱动的网络应用框架，提供web服务及客户端，获取远程节点上Block的集合
      */
    val blockTransferService = new NettyBlockTransferService(conf, securityManager, numUsableCores)

    /**
      * BlockManagerMaster负责对BlockManager的管理和协调，具体操作依赖于BlockManagerMasterEndpoint。
      * Drive和Executor处理BlockManagerMaster的方式不同：registerOrLookupEndpoint方法中
      *
      * 如果当前应用程序是Driver，则创建BlockManagerMaster，并且注册到RpcEnv中；
      * 如果当前应用程序是Executor，则从RpcEnv中找到BlockManagerMaster的引用。
      */
    val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
      new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
      conf, isDriver)

    // NB: blockManager is not valid until initialize() is called later.
    /**
      * 创建块管理器BlockManager
      *
      * BlockManager负责对Block的管理，只有在BlockManager的初始化方法initialize被调用后才是有效的。
      */
    val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
      serializer, conf, memoryManager, mapOutputTracker, shuffleManager,
      blockTransferService, securityManager, numUsableCores)

    /**
      * 创建广播管理器
      *
      * 用于将配置信息和序列化后的RDD、job以及ShuffleDependency等信息在本地存储。如果为了容灾，也会复制到其他节点上。
      * 参考：http://blog.csdn.net/w412692660/article/details/43639683
      */
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)

    /**
      * 用于缓存RDD某个分区计算后的中间结果，缓存计算的结果发生在迭代计算的时候。
      */
    val cacheManager = new CacheManager(blockManager)

    /**
      * Spark的测量系统，用于收集统计信息。
      * 包括excutor的状态，以及任务的状态。
      * 用于做监控工具很管用
      */
    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set("spark.executor.id", executorId)
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      ms.start()
      ms
    }

    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
    } else {
      "."
    }

    /**
      * 决定是否允许tasks将输出提交到HDFS上。Driver和Executor处理OutputCommitCoordinator的方式不同：
      *
      * 如果当前应用程序是Driver，则创建OutputCommitCoordinator，并注册到RpcEnv中；
      * 如果当前应用是Executor，从RpcUtils中查找到MapOutputTrackerMasterEndpoint的引用，当请求提交输出时会转到Driver的OutputCommitCoordinator。
      */
    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      actorSystem,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockTransferService,
      blockManager,
      securityManager,
      sparkFilesDir,
      metricsSystem,
      memoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) {
      envInstance.driverTmpDirToDelete = Some(sparkFilesDir)
    }

    envInstance
  }

  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
   */
  private[spark]
  def environmentDetails(
      conf: SparkConf,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains("spark.scheduler.mode")) {
        Seq(("spark.scheduler.mode", schedulingMode))
      } else {
        Seq[(String, String)]()
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
