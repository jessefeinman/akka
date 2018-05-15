/**
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import java.lang.reflect.Method

import akka.actor.{ ActorRef, ExtendedActorSystem }
import akka.remote.WireFormats.ActorRefData
import akka.serialization.{ Serialization, BaseSerializer }

import scala.collection.concurrent

object ProtobufSerializer {
  private val ARRAY_OF_BYTE_ARRAY = Array[Class[_]](classOf[Array[Byte]])

  /**
   * Helper to serialize an [[akka.actor.ActorRef]] to Akka's
   * protobuf representation.
   */
  def serializeActorRef(ref: ActorRef): ActorRefData = {
    ActorRefData.newBuilder.setPath(Serialization.serializedActorPath(ref)).build
  }

  /**
   * Helper to materialize (lookup) an [[akka.actor.ActorRef]]
   * from Akka's protobuf representation in the supplied
   * [[akka.actor.ActorSystem]].
   */
  def deserializeActorRef(system: ExtendedActorSystem, refProtocol: ActorRefData): ActorRef =
    system.provider.resolveActorRef(refProtocol.getPath)
}

/**
 * This Serializer serializes `akka.protobuf.Message` and `com.google.protobuf.Message`
 * It is using reflection to find the `parseFrom` and `toByteArray` methods to avoid
 * dependency to `com.google.protobuf`.
 */
class ProtobufSerializer(val system: ExtendedActorSystem) extends BaseSerializer {

  private val parsingMethodBindingRef = concurrent.TrieMap.empty[Class[_], Method]
  private val toByteArrayMethodBindingRef = concurrent.TrieMap.empty[Class[_], Method]

  override def includeManifest: Boolean = true

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case Some(clazz) ⇒
      parsingMethodBindingRef
        .getOrElseUpdate(clazz, clazz.getDeclaredMethod("parseFrom", ProtobufSerializer.ARRAY_OF_BYTE_ARRAY: _*))
        .invoke(null, bytes)
    case None ⇒ throw new IllegalArgumentException("Need a protobuf message class to be able to serialize bytes using protobuf")
  }

  override def toBinary(obj: AnyRef): Array[Byte] =
    toByteArrayMethodBindingRef
      .getOrElseUpdate(obj.getClass, obj.getClass.getMethod("toByteArray"))
      .invoke(obj).asInstanceOf[Array[Byte]]
}
