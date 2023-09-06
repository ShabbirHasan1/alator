use alator::clock::{Clock, ClockBuilder};
use alator::exchange::SingleExchangeBuilder;
use alator::input::{HashMapCorporateEventsSource, HashMapPriceSource};
use alator::strategy::StaticWeightStrategyBuilder;
use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;

use alator::broker::{BrokerCost, Dividend, Quote, SingleBroker, SingleBrokerBuilder};
use alator::simcontext::SimContextBuilder;
use alator::types::{CashValue, Frequency, PortfolioAllocation};

fn build_data(clock: Clock) -> HashMapPriceSource {
    let price_dist = Uniform::new(90.0, 100.0);
    let mut rng = thread_rng();

    let mut price_source = HashMapPriceSource::new(clock.clone());
    for date in clock.peek() {
        let q1 = Quote::new(
            price_dist.sample(&mut rng),
            price_dist.sample(&mut rng),
            date,
            "ABC",
        );
        let q2 = Quote::new(
            price_dist.sample(&mut rng),
            price_dist.sample(&mut rng),
            date,
            "BCD",
        );
        price_source.add_quotes(date, q1);
        price_source.add_quotes(date, q2);
    }
    price_source
}

#[test]
fn staticweight_integration_test() {
    env_logger::init();
    let initial_cash: CashValue = 100_000.0.into();
    let length_in_days: i64 = 1000;
    let start_date: i64 = 1609750800; //Date - 4/1/21 9:00:0000
    let clock = ClockBuilder::with_length_in_days(start_date, length_in_days)
        .with_frequency(&Frequency::Daily)
        .build();

    let price_source = build_data(clock.clone());

    let mut weights: PortfolioAllocation = PortfolioAllocation::new();
    weights.insert("ABC", 0.5);
    weights.insert("BCD", 0.5);

    let exchange = SingleExchangeBuilder::new()
        .with_price_source(price_source)
        .with_clock(clock.clone())
        .build();

    let simbrkr: SingleBroker<Dividend, HashMapCorporateEventsSource, Quote, HashMapPriceSource> =
        SingleBrokerBuilder::new()
            .with_trade_costs(vec![BrokerCost::Flat(1.0.into())])
            .with_exchange(exchange)
            .build();

    let strat = StaticWeightStrategyBuilder::new()
        .with_brkr(simbrkr)
        .with_weights(weights)
        .with_clock(clock.clone())
        .default();

    let mut sim = SimContextBuilder::new()
        .with_clock(clock.clone())
        .with_strategy(strat)
        .init(&initial_cash);

    sim.run();

    let _perf = sim.perf(Frequency::Daily);
}
