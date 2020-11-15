package com.github.mnogu.gatling.kafka

import com.github.mnogu.gatling.kafka.protocol.KafkaProtocolBuilder
import com.github.mnogu.gatling.kafka.request.builder.KafkaRequestBuilder
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session.Expression


object Predef {

  def kafka(implicit configuration: GatlingConfiguration): KafkaProtocolBuilder = KafkaProtocolBuilder(configuration)

  def kafka(requestName: Expression[String]): KafkaRequestBuilder = KafkaRequestBuilder(requestName)

}
