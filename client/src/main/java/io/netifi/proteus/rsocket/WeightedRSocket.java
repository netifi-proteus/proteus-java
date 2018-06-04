package io.netifi.proteus.rsocket;

import io.rsocket.RSocket;

/**
 * RSocket implementation that provides statistical weight around the connection including it's
 * latency and error rate. Used by ProteusBrokerService to
 * determine the next best RSocket to return.
 */
public interface WeightedRSocket extends RSocket {
  /**
   * Median value of latency as per last calculation. This is not calculated per invocation.
   *
   * @return Median latency.
   */
  double medianLatency();

  /**
   * Lower quantile of latency as per last calculation. This is not calculated per invocation.
   *
   * @return Median latency.
   */
  double lowerQuantileLatency();

  /**
   * Higher quantile value of latency as per last calculation. This is not calculated per
   * invocation.
   *
   * @return Median latency.
   */
  double higherQuantileLatency();

  /**
   * An exponentially weighted moving average value of the time between two requests.
   *
   * @return Inter arrival time.
   */
  double interArrivalTime();

  /**
   * Number of pending requests at this moment.
   *
   * @return Number of pending requests at this moment.
   */
  int pending();

  /**
   * Last time this socket was used i.e. either a request was sent or a response was received.
   *
   * @return Last time used in millis since epoch.
   */
  long lastTimeUsedMillis();

  /**
   * Returns the predicated latency of the weighted socket
   *
   * @return predicated latency in millis
   */
  double predictedLatency();

  /**
   * Error percentage caculated as an estimated weighted average
   *
   * @return the current error percentage
   */
  double errorPercentage();
}
