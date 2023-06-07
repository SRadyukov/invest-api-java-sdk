package ru.tinkoff.piapi.example;


import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import ru.tinkoff.piapi.contract.v1.Bond;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.CandleInstrument;
import ru.tinkoff.piapi.contract.v1.CandleSubscription;
import ru.tinkoff.piapi.contract.v1.Currency;
import ru.tinkoff.piapi.contract.v1.Etf;
import ru.tinkoff.piapi.contract.v1.InstrumentStatus;
import ru.tinkoff.piapi.contract.v1.InstrumentsRequest;
import ru.tinkoff.piapi.contract.v1.LastPrice;
import ru.tinkoff.piapi.contract.v1.MarketDataRequest;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.OrderBook;
import ru.tinkoff.piapi.contract.v1.OrderBookInstrument;
import ru.tinkoff.piapi.contract.v1.ReactorInstrumentsServiceGrpc;
import ru.tinkoff.piapi.contract.v1.ReactorMarketDataStreamServiceGrpc;
import ru.tinkoff.piapi.contract.v1.Share;
import ru.tinkoff.piapi.contract.v1.SubscribeCandlesRequest;
import ru.tinkoff.piapi.contract.v1.SubscribeOrderBookRequest;
import ru.tinkoff.piapi.contract.v1.SubscriptionAction;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
import ru.tinkoff.piapi.contract.v1.SubscriptionStatus;
import ru.tinkoff.piapi.core.InvestApi;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class ReactiveCandleStreamsExample {

  private static final int MAX_INSTRUMENT_PER_STREAM = 300;
  private final Map<String, Bond> bondMap = new ConcurrentHashMap<>();
  private final Map<String, Etf> etfMap = new ConcurrentHashMap<>();
  private final Map<String, Share> shareMap = new ConcurrentHashMap<>();
  private final Map<String, Currency> currencyMap = new ConcurrentHashMap<>();
  private final Map<String, LastPrice> instrumentLastPrices = new ConcurrentHashMap<>();
  private final Map<String, List<Candle>> instrumentCandles = new ConcurrentHashMap<>();
  private final Map<String, List<OrderBook>> instrumentOrderBook = new ConcurrentHashMap<>();
  private final ReactorInstrumentsServiceGrpc.ReactorInstrumentsServiceStub instrumentsServiceGrpc;
  private final ReactorMarketDataStreamServiceGrpc.ReactorMarketDataStreamServiceStub marketDataStreamServiceGrpc;
  private final Scheduler scheduler;
  private final AtomicInteger candleMsgCounter = new AtomicInteger(0);
  private final AtomicInteger orderBookMsgCounter = new AtomicInteger(0);

  ReactiveCandleStreamsExample(String token, String appName) {
    var channel = InvestApi.defaultChannel(token, appName);
    instrumentsServiceGrpc = ReactorInstrumentsServiceGrpc.newReactorStub(channel);
    marketDataStreamServiceGrpc = ReactorMarketDataStreamServiceGrpc.newReactorStub(channel);
    scheduler = Schedulers.newBoundedElastic(2, 1_000, "reactive-exampl-scheduler");
  }


  public Mono<Void> run() {
    var instrumentsReq = InstrumentsRequest.newBuilder()
      .setInstrumentStatus(InstrumentStatus.INSTRUMENT_STATUS_BASE).build();

    var started = System.currentTimeMillis();

    return instrumentsServiceGrpc.currencies(instrumentsReq)
      .flux()
      .flatMap(
        currenciesResponse -> Flux.fromStream(
          currenciesResponse
            .getInstrumentsList()
            .stream()
        )
      ).doOnNext(t -> {
        currencyMap.put(t.getUid(), t);
      })
      .then(Mono.defer(() -> Flux.zip(
            instrumentsServiceGrpc.bonds(instrumentsReq)
              .flux()
              .flatMap(
                bondsResponse -> Flux.fromStream(
                  bondsResponse
                    .getInstrumentsList()
                    .stream()
                )
              ),
            instrumentsServiceGrpc.shares(instrumentsReq)
              .flux()
              .flatMap(
                sharesResponse -> Flux.fromStream(
                  sharesResponse
                    .getInstrumentsList()
                    .stream()
                )
              )

          )
          .doOnNext(t -> {
            bondMap.put(t.getT1().getUid(), t.getT1());
            shareMap.put(t.getT2().getUid(), t.getT2());
          })
          .then()
      ))
      .then(Mono.defer(() -> {

        var sharesChunks = shareMap.values()
          .stream()
          .filter(Share::getApiTradeAvailableFlag)
          .limit(900)
          .collect(Collectors.toList());


        return Flux.fromIterable(sharesChunks)
          .buffer(MAX_INSTRUMENT_PER_STREAM)
          .flatMap(sharesChunk -> {

            Sinks.Many<MarketDataRequest> mdCandlesRequestSink = Sinks.many().unicast().onBackpressureError();
            var candlesResponseFlux = marketDataStreamServiceGrpc
              .marketDataStream(
                Flux.concat(Flux.just(MarketDataRequest.newBuilder()
                  .setSubscribeCandlesRequest(SubscribeCandlesRequest.newBuilder()
                    .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
                    .addAllInstruments(
                      sharesChunk.stream()
                        .map(share -> CandleInstrument.newBuilder()
                          .setInstrumentId(share.getUid())
                          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
                          .build())
                        .collect(Collectors.toList())
                    )
                    .build())
                  .build()), mdCandlesRequestSink.asFlux())
              );

            Sinks.Many<MarketDataRequest> mdOrderbookRequestSink = Sinks.many().unicast().onBackpressureError();
            var orderbookResponseFlux = marketDataStreamServiceGrpc
              .marketDataStream(
                Flux.concat(
                  Flux.just(MarketDataRequest.newBuilder()
                    .setSubscribeOrderBookRequest(SubscribeOrderBookRequest.newBuilder()
                      .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
                      .addAllInstruments(
                        sharesChunk.stream()
                          .map(share -> OrderBookInstrument.newBuilder()
                            .setInstrumentId(share.getUid())
                            .setDepth(10)
                            .build())
                          .collect(Collectors.toList())
                      )
                      .build())
                    .build())
                )
              );

            var disposeCandlesStream = candlesResponseFlux
              .subscribe((mdResponse) -> {
                  log.debug("received candle market data stream :{}", mdResponse);
                  processMarketDataResponse(mdResponse);
                },
                error -> log.error("error subscribe to candle market data stream", error),
                () -> log.info("candle market data stream complete")
              );

            var disposeOrderBookStream = orderbookResponseFlux
              .subscribe((mdResponse) -> {
                  log.debug("received orderbook market data stream :{}", mdResponse);
                  processMarketDataResponse(mdResponse);
                },
                error -> log.error("error subscribe to orderbook market data stream", error),
                () -> log.info("orderbook market data stream complete")
              );

            //next we just close streams after timeout
            return Flux.just(disposeCandlesStream, disposeOrderBookStream);
          })
          .collectList()
          .delayElement(Duration.ofSeconds(1200))
          .doOnNext(disposables -> {
            log.warn("took:{} sec, totalmsg:{}, candle_count:{} orderbook_count:{} candle avg rps:{} orderbook_avg_rps:{}",
              TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started),
              candleMsgCounter.get() + orderBookMsgCounter.get(),
              candleMsgCounter.get(),
              orderBookMsgCounter.get(),
              Math.round(1d * candleMsgCounter.get() / TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started)),
              Math.round(1d * orderBookMsgCounter.get() / TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started))
            );
            disposables.forEach(Disposable::dispose);
          })
          .then();

      }).then());


  }

  private void processMarketDataResponse(MarketDataResponse mdResponse) {
    if (mdResponse.hasSubscribeCandlesResponse()) {
      var count = mdResponse.getSubscribeCandlesResponse()
        .getCandlesSubscriptionsList().stream()
        .filter(cs -> cs.getSubscriptionStatus() == SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
        .count();
      if (count < MAX_INSTRUMENT_PER_STREAM) {
        log.warn("candle subscription count:{}", count);
      }
    } else if (mdResponse.hasCandle()) {
      var newCandle = mdResponse.getCandle();
      var instrumentUid = newCandle.getInstrumentUid();
      var existsCandles = instrumentCandles.getOrDefault(instrumentUid, List.of());
      if (existsCandles.size() > 1 && !checkLastTradeDt(existsCandles.get(0), newCandle)) {
        log.error("candle instrument: {} has staled lastTradeDt: {} exists one:{}"
          , newCandle.getInstrumentUid()
          , newCandle.getLastTradeTs()
          , existsCandles.get(0).getLastTradeTs()
        );
      }

      var mutateCandleslist = existsCandles.stream().limit(100).collect(Collectors.toCollection(ArrayList::new));
      mutateCandleslist.add(newCandle);
      instrumentCandles.put(instrumentUid, List.copyOf(mutateCandleslist));
      candleMsgCounter.incrementAndGet();

    } else if (mdResponse.hasSubscribeOrderBookResponse()) {
      var count = mdResponse.getSubscribeOrderBookResponse()
        .getOrderBookSubscriptionsList()
        .stream()
        .filter(cs -> cs.getSubscriptionStatus() == SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
        .count();
      if (count < MAX_INSTRUMENT_PER_STREAM) {
        log.warn("orderbook subscription count:{}", count);
      }
    } else if (mdResponse.hasOrderbook()) {
      var orderbook = mdResponse.getOrderbook();
      var instrumentUid = orderbook.getInstrumentUid();
      var existsOrderBooks = instrumentOrderBook.getOrDefault(instrumentUid, List.of());
      var mutateOrderBooks = existsOrderBooks.stream().limit(100).collect(Collectors.toList());
      mutateOrderBooks.add(orderbook);
      instrumentOrderBook.put(instrumentUid, List.copyOf(mutateOrderBooks));
      orderBookMsgCounter.incrementAndGet();
    }
  }


  private boolean checkLastTradeDt(Candle candle, Candle newCandle) {
    var nlts = newCandle.getLastTradeTs();
    var elts = candle.getLastTradeTs();
    return nlts.getSeconds() > elts.getSeconds() ||
      (nlts.getSeconds() == elts.getSeconds() && nlts.getNanos() > elts.getNanos());
  }


}
