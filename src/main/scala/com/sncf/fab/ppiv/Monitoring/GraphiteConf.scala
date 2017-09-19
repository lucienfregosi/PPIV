package com.sncf.fab.ppiv.Monitoring

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit._
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{MetricFilter, SharedMetricRegistries}
import com.sncf.fab.ppiv.utils.AppConf


object GraphiteConf {

  lazy val config = AppConf

  val graphite = new Graphite(new InetSocketAddress(
    config.metricHost, config.metricPort))

  lazy val prefix: String = config.metricPrefix

  val reporter = GraphiteReporter.forRegistry(
    SharedMetricRegistries.getOrCreate(prefix))
    .prefixedWith(s"$prefix.${java.net.InetAddress.getLocalHost.getHostName}")
    .convertRatesTo(SECONDS)
    .convertDurationsTo(MILLISECONDS)
    .filter(MetricFilter.ALL)
    .build(graphite)

    def startGraphite(): Unit = {
    if (config.metricEnabled) {
      reporter.start(config.metricRefreshInterval, SECONDS)
    }
  }
}