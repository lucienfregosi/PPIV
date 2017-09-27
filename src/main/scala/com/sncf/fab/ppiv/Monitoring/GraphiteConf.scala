package com.sncf.fab.ppiv.Monitoring
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit._

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{Gauge, MetricFilter, MetricRegistry, SharedMetricRegistries}
import com.codahale.metrics
import com.sncf.fab.ppiv.utils.AppConf

// Objet de configuration pour Graphite
object GraphiteConf {

  lazy val config = AppConf

  val graphite = new Graphite(new InetSocketAddress(
    config.metricHost, config.metricPort))



  lazy val prefix: String = config.metricPrefix


  var registry = SharedMetricRegistries.getOrCreate("default_test")


  val reporter = GraphiteReporter.
    forRegistry(registry)
    .prefixedWith(s"$prefix")
    .convertRatesTo(SECONDS)
    .convertDurationsTo(MILLISECONDS)
    .filter(MetricFilter.ALL)
    .build(graphite)


    def startGraphite(): Unit = {
    if (config.metricEnabled) {
      reporter.start(config.metricRefreshInterval, SECONDS)
      registry.counter("cpu")

    }
  }
}
