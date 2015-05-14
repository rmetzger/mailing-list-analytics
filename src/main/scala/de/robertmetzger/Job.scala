package de.robertmetzger

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.charset.Charset

import org.apache.flink.api.scala._
import scala.collection.JavaConversions._
import org.apache.james.mime4j.mboxiterator.{CharBufferWrapper, MboxIterator}
import org.apache.james.mime4j.message.DefaultMessageBuilder

/**
 * Based on https://svn.apache.org/repos/asf/james/mime4j/trunk/examples/src/main/java/org/apache/james/mime4j/samples/mbox/IterateOverMbox.java
 */
object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.createCollectionsEnvironment

    val encoding = Charset.forName("UTF-8");
    // read messages locally into a list:
    val mboxFile = "/media/green/data/Dropbox/Dropbox/apache-mail-archives/flink-dev/201502.mbox"
    val mboxIterator = MboxIterator.fromFile(mboxFile).charset(encoding).build().iterator().toSeq
    /*val list = for(messageChars <- mboxIterator) {
      val builder = new DefaultMessageBuilder()
      val message = builder.parseMessage(messageChars.asInputStream(encoding))
      println("Subject: "+message.getSubject)
    } yield  { 1 } */
    val list = mboxIterator.map(messageChars => {
      val builder = new DefaultMessageBuilder()
      val message = builder.parseMessage(messageChars.asInputStream(encoding))
      println("Subject: "+message.getSubject)
      message
    })

    val messagesDs = env.fromCollection(list)
    val byDay = messagesDs.groupBy( el => {el.getDate.getDay})
    

    // execute program
    //env.execute("Flink Scala API Skeleton")
  }
}
