package com.sncf.fab.ppiv.Monitoring

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit._

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{Gauge, MetricFilter, MetricRegistry, SharedMetricRegistries}
import com.sncf.fab.ppiv.utils.AppConf


object GraphiteConf {

  lazy val config = AppConf

  val graphite = new Graphite(new InetSocketAddress(
    config.metricHost, config.metricPort))


  lazy val prefix: String = config.metricPrefix

  import com.codahale.metrics.MetricRegistry

  val metrics = new MetricRegistry

  var registry = SharedMetricRegistries.getOrCreate("default_test")

  val reporter = GraphiteReporter.
    forRegistry(registry)
    //.prefixedWith(s"$prefix.${java.net.InetAddress.getLocalHost.getHostName}")
    .prefixedWith(s"$prefix.${java.net.InetAddress.getLocalHost.getHostName}")
    .convertRatesTo(SECONDS)
    .convertDurationsTo(MILLISECONDS)
    .filter(MetricFilter.ALL)
    .build(graphite)


    def startGraphite(): Unit = {
    if (config.metricEnabled) {
      println("GRAPHITE STARTED")
       println(graphite.isConnected())
      reporter.start(config.metricRefreshInterval, SECONDS)
      reporter.report()
      println(graphite.isConnected())
      registry.counter("cpu")
    }
  }
}