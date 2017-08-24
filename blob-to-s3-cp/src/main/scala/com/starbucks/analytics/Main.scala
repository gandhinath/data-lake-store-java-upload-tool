package com.starbucks.analytics

import com.microsoft.azure.eventprocessorhost.{EventProcessorHost, EventProcessorOptions}
import com.starbucks.analytics.blob.BlobConnectionInfo
import com.starbucks.analytics.eventhub._
import com.starbucks.analytics.keyvault.KeyVaultConnectionInfo
import com.starbucks.analytics.s3.S3ConnectionInfo
import com.typesafe.scalalogging.Logger

import scala.concurrent.ExecutionException
import scala.util.{Failure, Success}


/**
  * Main class to start file transfer from Azure blob to AWS S3.
  */
object Main {
  private val logger: Logger = Logger("Main")

  /**
    * Entry point
    * @param args Command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new s3cpConf(args)
    logger.info(conf.summary)

    //Setup Azure EventHub Connection
    val eventHubConnectionInfo = EventHubConnectionInfo(
           eventHubNamespaceName = conf.eventHubNamespaceName(),
          eventHubName = conf.eventHubName(),
         sasKeyName = conf.eventHubSASKeyName(),
          sasKey = conf.eventHubSASKey(),
          eventProcessorName = conf.eventProcessorName(),
          eventProcessorStorageContainer = conf.eventProcessorStorageContainer(),
          consumerGroup = conf.eventHubConsumerGroup()
    )

    //Setup AWS S3 Connection
    val s3ConnectionInfo = S3ConnectionInfo(
      awsAccessKeyID = conf.awsAccessKeyID(),
      awsSecretAccessKey = conf.awsSecretAccessKey(),
      s3BucketName = conf.s3BucketName(),
      s3FolderName = conf.s3FolderName()
    )

    //Setup Azure Storage connection for EventProcessor.
    val blobConnectionInfo = BlobConnectionInfo (
      accountName = conf.eventHubStorageAccountName(),
      accountKey = conf.eventHubStorageAccountKey()
    )

    val keyVaultConnectionInfo = KeyVaultConnectionInfo(
      clientId = conf.spnClientId(),
      clientKey = conf.spnClientKey()
    )

    EventHubManager.getEventProcessorHost(eventHubConnectionInfo, blobConnectionInfo) match {
      case Failure(e) => println(e)
      case Success(eventProcessorHost) => {
        val options = new EventProcessorOptions
        options.setExceptionNotification(new EventHubErrorNotificationHandler)
        try{
          eventProcessorHost.registerEventProcessorFactory(new EventProcessorFactory(s3ConnectionInfo, keyVaultConnectionInfo, conf.keyVaultResourceUri() ,conf.desiredParallelism()), options)
        }catch {
          case e: ExecutionException => {
            logger.error(e.getCause.toString)
          }
          case e: Exception => {
            logger.error(e.getCause.toString)
          }
        }
        // SIGNAL LISTENER TO GRACEFULLY TERMINATE EVENT PROCESSOR.
        println("Press any key to stop: ")
        try{
          System.in.read()
          eventProcessorHost.unregisterEventProcessor()
          logger.warn("Calling forceExecutorShutdown")
          EventProcessorHost.setAutoExecutorShutdown(true)
          logger.info("EventProcessorHost existed gracefully!")
        }catch {
          case e: Exception => {
            logger.error(e.getCause.toString)
            System.exit(1)
          }
        }
      }
    }


//    // Method to download the blob file and get decrypted content.
//    def decryptDownload(blobConnectionInfo: BlobConnectionInfo,
//                        keyVaultConnectionInfo: KeyVaultConnectionInfo,
//                        conf: Conf
//                       ): KeyVaultKeyResolver = {
//
//      // Generate the file name
//      var blobFileName = conf.blobStoreRootFolder.getOrElse("")
//      if (blobFileName.startsWith("/"))
//        blobFileName = blobFileName.drop(1)
//      if (blobFileName.length > 0) {
//        if (!blobFileName.endsWith("/"))
//          blobFileName += "/"
//      }
//
//      //    if (!sourceFile.startsWith("/"))
//      //      blobFileName = s"$blobFileName$sourceFile"
//      //    else
//      //      blobFileName = s"$blobFileName${sourceFile.drop(1)}"
//
//      val keyVaultResolver = new KeyVaultKeyResolver(KeyVaultManager.getKeyVaultKeyClient(keyVaultConnectionInfo, conf.keyVaultResourceUri()))
//      keyVaultResolver
//    }
//
//
//
//
//    // Start the download of the encrypted file from blob.
//
//    val keyVaultResolver = decryptDownload(blobConnectionInfo, keyVaultConnectionInfo, conf)
//
//
//
//    // do the decrypt download here.
//    eventsToPublish.foreach(event => {
//      logger.info(s"Start copying for file ${event.uri}")
//      val uris = event.uri.split(";")
//      val primaryUri = uris(0).split(" = ")(1).trim
//      val secondaryUri = uris(1).split(" = ")(1).trim
//      val sasUri = primaryUri.substring(1, primaryUri.length - 1) + "?" + event.sharedAccessSignatureToken.trim
//      logger.info("SAS URI for the blob is : " + sasUri)
//
//      // Method to create and get Aure blob InputStream, blobName and blobSize.
//      def getBlobStream(azureBlockBlob: CloudBlockBlob): (BlobInputStream, String, Long) = {
//        val blobEncryptionPolicy = new BlobEncryptionPolicy(null, keyVaultResolver)
//        val blobRequestOptions = new BlobRequestOptions()
//        val operationContext = new OperationContext()
//        blobRequestOptions.setEncryptionPolicy(blobEncryptionPolicy)
//        blobRequestOptions.setConcurrentRequestCount(100)
//        operationContext.setLoggingEnabled(true)
//        // get the blob file metadata.
//        azureBlockBlob.downloadAttributes()
//
//        println(azureBlockBlob.downloadText(null, null, blobRequestOptions, operationContext))
//        azureBlockBlob.download(new FileOutputStream("/Users/depatel/text2.txt"))
//
//        val os: ByteArrayOutputStream = new ByteArrayOutputStream()
//        azureBlockBlob.download(os, null, blobRequestOptions, null)
//        println(os.size() + " " + os.toByteArray.length)
//        (azureBlockBlob.openInputStream(), azureBlockBlob.getName, azureBlockBlob.getProperties.getLength)
//      }
//
//      BlobManager.withSASUriBlobReference(sasUri, getBlobStream)
//    })


    System.exit(0)
  }

  case class decryptDownload

}
