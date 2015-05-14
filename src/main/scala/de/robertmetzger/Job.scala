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
import java.util.Date

import org.apache.flink.api.scala._
import scala.collection.JavaConversions._
import org.apache.james.mime4j.mboxiterator.{CharBufferWrapper, MboxIterator}
import org.apache.james.mime4j.message.DefaultMessageBuilder

/**
 * Based on https://svn.apache.org/repos/asf/james/mime4j/trunk/examples/src/main/java/org/apache/james/mime4j/samples/mbox/IterateOverMbox.java
 */
object Job {

  val ml_keys = List("machine learning", "ml")
  val python_keys = List("python", "pyspark")
  val scala_keys = List("Scala")
  val java_keys = List("Java")
  val sql_keys = List("SQL", "sparksql")
  val streaming_keys = List("Streaming", "Kafka")
  val mesos_key = List("mesos")

  /**
   * Contains the frequencies of the different topics
   */
  case class Result(ml: Int,
                    python: Int,
                    scala: Int,
                    java: Int,
                    sql: Int,
                    streaming: Int,
                    yarn: Int,
                    mesos: Int) {
    def this() = this(0,0,0,0,0,0,0,0)
  }

  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.createCollectionsEnvironment

    val encoding = Charset.forName("UTF-8");
    // read messages locally into a list:
    val mboxFile = "/media/green/data/Dropbox/Dropbox/apache-mail-archives/flink-dev/201502.mbox"
    val mboxIterator = MboxIterator.fromFile(mboxFile).charset(encoding).build().iterator().toSeq

    val list = mboxIterator.map(messageChars => {
      val builder = new DefaultMessageBuilder()
      val message = builder.parseMessage(messageChars.asInputStream(encoding))
      (message.getSubject, message.getDate)
    })

    val messagesDs = env.fromCollection(list)
    val byDay = messagesDs.groupBy( el => {el._2.getDay})
    val result = byDay.reduceGroup( messages => {
      var day: Date = null
      val results = messages.map( message => {
        day = message._2
        var result = new Result()
        var subj = message._1
        // TODO here
        result
      }).toSeq
      (day, results)
    })

    result.print
    // execute program
    env.execute("Flink Scala API Skeleton")
  }
}


