mod calculations;
mod concurrent_impl;
mod record;
mod single_impl;
mod types;

pub use calculations::BrokerCalculations;
pub use concurrent_impl::{ConcurrentBroker, ConcurrentBrokerBuilder};
pub use record::BrokerLog;
pub use single_impl::{SingleBroker, SingleBrokerBuilder};
pub use types::{
    BrokerCashEvent, BrokerCost, BrokerEvent, BrokerRecordedEvent, Dividend, DividendPayment,
    Order, OrderType, Quote, Trade, TradeType,
};

#[cfg(feature = "python")]
pub use types::{PyDividend, PyQuote};

use async_trait::async_trait;
use log::info;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::input::Quotable;
use crate::types::{CashValue, PortfolioHoldings, PortfolioQty, PortfolioValues, Price};
//Key traits for broker implementations.
//
//Whilst broker is implemented within this package as a singular broker, the intention of these
//traits is to hide the implementation from the user so that it could be one or a combination of
//brokers returning the data. Similarly, strategy implementations should not create any
//dependencies on the underlying state of the broker.
//
///Represents functionality that all brokers have to support in order to perform any backtests at
///all. Implementations may choose not to implement some part of this functionality but this trait
///represents a general base case.
///
///In practice, the only optional trait that seems to be included often is [GetsQuote]. This is not
///included in the base definition because turning the broker into both a source and user of data
///is implementation-dependent.
///
///This confusion in the implementation is also why get_position_value requires mutation: broker
///not only asks for prices but keeps state about prices so that we can find a valuation for the
///security if we are missing a price for the current date. At some point, we make relax this
///mutability constraint.
///
///Clients should not be able to call debit or credit themselves. Deposits or withdrawals are
///implemented through [TransferCash] trait.
#[async_trait]
pub trait BacktestBroker {
    fn get_position_profit(&self, symbol: &str) -> Option<CashValue> {
        if let Some(cost) = self.get_position_cost(symbol) {
            if let Some(position_value) = self.get_position_value(symbol) {
                if let Some(qty) = self.get_position_qty(symbol) {
                    let price = *position_value / *qty.clone();
                    let value = CashValue::from(*qty.clone() * (price - *cost));
                    return Some(value);
                }
            }
        }
        None
    }

    fn get_position_liquidation_value(&self, symbol: &str) -> Option<CashValue> {
        //TODO: we need to introduce some kind of distinction between short and long
        //      positions.
        if let Some(position_value) = self.get_position_value(symbol) {
            if let Some(qty) = self.get_position_qty(symbol) {
                let price = Price::from(*position_value / **qty);
                let (value_after_costs, _price_after_costs) =
                    self.calc_trade_impact(&position_value, &price, false);
                return Some(value_after_costs);
            }
        }
        None
    }
    fn get_total_value(&self) -> CashValue {
        let assets = self.get_positions();
        let mut value = self.get_cash_balance();
        for a in assets {
            if let Some(position_value) = self.get_position_value(&a) {
                value = CashValue::from(*value + *position_value);
            }
        }
        value
    }

    fn get_liquidation_value(&self) -> CashValue {
        let mut value = self.get_cash_balance();
        for asset in self.get_positions() {
            if let Some(asset_value) = self.get_position_liquidation_value(&asset) {
                value = CashValue::from(*value + *asset_value);
            }
        }
        value
    }

    fn get_values(&self) -> PortfolioValues {
        let mut holdings = PortfolioValues::new();
        let assets = self.get_positions();
        for a in assets {
            let value = self.get_position_value(&a);
            if let Some(v) = value {
                holdings.insert(&a, &v);
            }
        }
        holdings
    }

    fn get_cash_balance(&self) -> CashValue;
    //TODO: Position qty can always return a value, if we don't have the position then qty is 0
    fn get_position_qty(&self, symbol: &str) -> Option<&PortfolioQty>;
    //TODO: Position value can always return a value, if we don't have a position then value is 0
    fn get_position_value(&self, symbol: &str) -> Option<CashValue>;
    fn get_position_cost(&self, symbol: &str) -> Option<Price>;
    fn get_positions(&self) -> Vec<String>;
    fn get_holdings(&self) -> PortfolioHoldings;
    //This should only be called internally
    fn get_trade_costs(&self, trade: &types::Trade) -> CashValue;
    fn calc_trade_impact(&self, budget: &f64, price: &f64, is_buy: bool) -> (CashValue, Price);
    fn update_holdings(&mut self, symbol: &str, change: PortfolioQty);
    fn pay_dividends(&mut self);
    fn debit(&mut self, value: &f64) -> types::BrokerCashEvent;
    fn credit(&mut self, value: &f64) -> types::BrokerCashEvent;
    //Can leave the client with a negative cash balance
    fn debit_force(&mut self, value: &f64) -> types::BrokerCashEvent;
}

///Implementation allows clients to alter the cash balance through withdrawing or depositing cash.
///This does not come with base implementation because clients may wish to restrict this behaviour.
pub trait TransferCash: BacktestBroker {
    fn withdraw_cash(&mut self, cash: &f64) -> types::BrokerCashEvent {
        if cash > &self.get_cash_balance() {
            info!(
                "BROKER: Attempted cash withdraw of {:?} but only have {:?}",
                cash,
                self.get_cash_balance()
            );
            return types::BrokerCashEvent::WithdrawFailure(CashValue::from(*cash));
        }
        info!(
            "BROKER: Successful cash withdraw of {:?}, {:?} left in cash",
            cash,
            self.get_cash_balance()
        );
        self.debit(cash);
        types::BrokerCashEvent::WithdrawSuccess(CashValue::from(*cash))
    }

    fn deposit_cash(&mut self, cash: &f64) -> types::BrokerCashEvent {
        info!(
            "BROKER: Deposited {:?} cash, current balance of {:?}",
            cash,
            self.get_cash_balance()
        );
        self.credit(cash);
        types::BrokerCashEvent::DepositSuccess(CashValue::from(*cash))
    }
}

pub trait ReceievesOrders {
    //TODO: this needs to use another kind of order
    fn send_order(&mut self, order: types::Order) -> types::BrokerEvent;
    fn send_orders(&mut self, order: &[types::Order]) -> Vec<types::BrokerEvent>;
}

#[async_trait]
pub trait ReceievesOrdersAsync {
    //TODO: this needs to use another kind of order
    async fn send_order(&mut self, order: types::Order) -> types::BrokerEvent;
    async fn send_orders(&mut self, order: &[types::Order]) -> Vec<types::BrokerEvent>;
}

//Implementation allows clients to retrieve prices. This trait may be used to retrieve prices
//internally too, and this confusion comes from broker implementations being both a consumer and
//source of data. So this trait is seperated out now but may disappear in future versions.
pub trait GetsQuote<Q: Quotable> {
    fn get_quote(&self, symbol: &str) -> Option<Arc<Q>>;
    fn get_quotes(&self) -> Option<Vec<Arc<Q>>>;
}

///Implementation allows clients to query properties of the transaction history of the broker.
///Again, this is an optional feature but is useful for things like tax calculations.
///
///When using this note that it offers operations that are distinct in purpose from a performance
///calculation. Performance statistics are created at the end of a backtest but the intention here
///is to provide a view into transactions whilst the simulation is still running i.e. for tax
///calculations.
pub trait EventLog {
    fn trades_between(&self, start: &i64, end: &i64) -> Vec<types::Trade>;
    fn dividends_between(&self, start: &i64, end: &i64) -> Vec<types::DividendPayment>;
}

#[derive(Debug, Clone)]
pub struct InsufficientCashError;

impl Error for InsufficientCashError {}

impl Display for InsufficientCashError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Client has insufficient cash to execute order")
    }
}

#[derive(Debug, Clone)]
pub struct UnexecutableOrderError;

impl Error for UnexecutableOrderError {}

impl Display for UnexecutableOrderError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Client has passed unexecutable order")
    }
}

#[cfg(test)]
mod tests {

    use super::{
        BacktestBroker, BrokerCalculations, BrokerCost, ConcurrentBrokerBuilder, OrderType, Quote,
        TransferCash,
    };
    use crate::broker::{ConcurrentBroker, Dividend, ReceievesOrdersAsync};
    use crate::exchange::ConcurrentExchangeBuilder;
    use crate::input::{
        fake_price_source_generator, HashMapCorporateEventsSource, HashMapPriceSource,
    };
    use crate::types::PortfolioAllocation;
    use crate::{clock::ClockBuilder, types::Frequency};

    #[tokio::test]
    async fn diff_direction_correct_if_need_to_buy() {
        let clock = ClockBuilder::with_length_in_days(0, 10)
            .with_frequency(&Frequency::Daily)
            .build();
        let price_source = fake_price_source_generator(clock.clone());

        let mut exchange = ConcurrentExchangeBuilder::new()
            .with_clock(clock.clone())
            .with_price_source(price_source)
            .build();

        let mut brkr: ConcurrentBroker<Dividend, HashMapCorporateEventsSource, Quote> =
            ConcurrentBrokerBuilder::new()
                .with_trade_costs(vec![BrokerCost::flat(1.0)])
                .build(&mut exchange)
                .await;

        let mut weights = PortfolioAllocation::new();
        weights.insert("ABC", 1.0);

        brkr.deposit_cash(&100_000.0);
        exchange.check().await;
        brkr.check().await;

        let orders = BrokerCalculations::diff_brkr_against_target_weights(&weights, &mut brkr);
        println!("{:?}", orders);
        let first = orders.first().unwrap();
        assert!(matches!(
            first.get_order_type(),
            OrderType::MarketBuy { .. }
        ));
    }

    #[tokio::test]
    async fn diff_direction_correct_if_need_to_sell() {
        //This is connected to the previous test, if the above fails then this will never pass.
        //However, if the above passes this could still fail.
        let clock = ClockBuilder::with_length_in_days(0, 10)
            .with_frequency(&Frequency::Daily)
            .build();

        let price_source = fake_price_source_generator(clock.clone());

        let mut exchange = ConcurrentExchangeBuilder::new()
            .with_clock(clock.clone())
            .with_price_source(price_source)
            .build();

        let mut brkr: ConcurrentBroker<Dividend, HashMapCorporateEventsSource, Quote> =
            ConcurrentBrokerBuilder::new()
                .with_trade_costs(vec![BrokerCost::flat(1.0)])
                .build(&mut exchange)
                .await;

        let mut weights = PortfolioAllocation::new();
        weights.insert("ABC", 1.0);

        brkr.deposit_cash(&100_000.0);
        let orders = BrokerCalculations::diff_brkr_against_target_weights(&weights, &mut brkr);
        brkr.send_orders(&orders).await;

        exchange.check().await;
        brkr.check().await;

        exchange.check().await;
        brkr.check().await;

        let mut weights1 = PortfolioAllocation::new();
        //This weight needs to very small because it is possible for the data generator to generate
        //a price that drops significantly meaning that rebalancing requires a buy not a sell. This
        //is unlikely but seems to happen eventually.
        weights1.insert("ABC", 0.01);
        let orders1 = BrokerCalculations::diff_brkr_against_target_weights(&weights1, &mut brkr);

        println!("{:?}", orders1);
        let first = orders1.first().unwrap();
        assert!(matches!(
            first.get_order_type(),
            OrderType::MarketSell { .. }
        ));
    }

    #[tokio::test]
    async fn diff_continues_if_security_missing() {
        //In this scenario, the user has inserted incorrect information but this scenario can also occur if there is no quote
        //for a given security on a certain date. We are interested in the latter case, not the former but it is more
        //difficult to test for the latter, and the code should be the same.
        let clock = ClockBuilder::with_length_in_days(0, 10)
            .with_frequency(&Frequency::Daily)
            .build();

        let price_source = fake_price_source_generator(clock.clone());
        let mut exchange = ConcurrentExchangeBuilder::new()
            .with_clock(clock.clone())
            .with_price_source(price_source)
            .build();
        let mut brkr: ConcurrentBroker<Dividend, HashMapCorporateEventsSource, Quote> =
            ConcurrentBrokerBuilder::new()
                .with_trade_costs(vec![BrokerCost::flat(1.0)])
                .build(&mut exchange)
                .await;

        let mut weights = PortfolioAllocation::new();
        weights.insert("ABC", 0.5);
        //There is no quote for this security in the underlying data, code should make the assumption (that doesn't apply here)
        //that there is some quote for this security at a later date and continues to generate order for ABC without throwing
        //error
        weights.insert("XYZ", 0.5);

        brkr.deposit_cash(&100_000.0);
        exchange.check().await;
        brkr.check().await;
        let orders = BrokerCalculations::diff_brkr_against_target_weights(&weights, &mut brkr);
        assert!(orders.len() == 1);
    }

    #[tokio::test]
    #[should_panic]
    async fn diff_panics_if_brkr_has_no_cash() {
        //If we get to a point where the client is diffing without cash, we can assume that no further operations are possible
        //and we should panic
        let clock = ClockBuilder::with_length_in_days(0, 10)
            .with_frequency(&Frequency::Daily)
            .build();

        let price_source = fake_price_source_generator(clock.clone());
        let mut exchange = ConcurrentExchangeBuilder::new()
            .with_clock(clock.clone())
            .with_price_source(price_source)
            .build();
        let mut brkr: ConcurrentBroker<Dividend, HashMapCorporateEventsSource, Quote> =
            ConcurrentBrokerBuilder::new()
                .with_trade_costs(vec![BrokerCost::flat(1.0)])
                .build(&mut exchange)
                .await;

        let mut weights = PortfolioAllocation::new();
        weights.insert("ABC", 1.0);

        exchange.check().await;
        brkr.check().await;
        BrokerCalculations::diff_brkr_against_target_weights(&weights, &mut brkr);
    }

    #[test]
    fn can_estimate_trade_costs_of_proposed_trade() {
        let pershare = BrokerCost::per_share(0.1);
        let flat = BrokerCost::flat(10.0);
        let pct = BrokerCost::pct_of_value(0.01);

        let res = pershare.trade_impact(&1000.0, &1.0, true);
        assert!((*res.1).eq(&1.1));

        let res = pershare.trade_impact(&1000.0, &1.0, false);
        assert!((*res.1).eq(&0.9));

        let res = flat.trade_impact(&1000.0, &1.0, true);
        assert!((*res.0).eq(&990.00));

        let res = pct.trade_impact(&100.0, &1.0, true);
        assert!((*res.0).eq(&99.0));

        let costs = vec![pershare, flat];
        let initial = BrokerCost::trade_impact_total(&costs, &1000.0, &1.0, true);
        assert!((*initial.0).eq(&990.00));
        assert!((*initial.1).eq(&1.1));
    }

    #[tokio::test]
    async fn diff_handles_sent_but_unexecuted_orders() {
        //It is possible for the client to issue orders for infinitely increasing numbers of shares
        //if there is a gap between orders being issued and executed. For example, if we are
        //missing price data the client could think we need 100 shares, that order doesn't get
        //executed on the next tick, and the client then issues orders for another 100 shares.
        //
        //This is not possible without earlier price data either. If there is no price data then
        //the diff will be unable to work out how many shares are required. So the test case is
        //some price but no price for the execution period.
        let clock = ClockBuilder::with_length_in_seconds(100, 5)
            .with_frequency(&Frequency::Second)
            .build();
        let mut price_source = HashMapPriceSource::new(clock.clone());
        price_source.add_quotes(100, Quote::new(100.00, 100.00, 100, "ABC"));
        price_source.add_quotes(101, Quote::new(100.00, 100.00, 101, "ABC"));
        price_source.add_quotes(103, Quote::new(100.00, 100.00, 103, "ABC"));

        let mut exchange = ConcurrentExchangeBuilder::new()
            .with_clock(clock.clone())
            .with_price_source(price_source)
            .build();

        let mut brkr: ConcurrentBroker<Dividend, HashMapCorporateEventsSource, Quote> =
            ConcurrentBrokerBuilder::new().build(&mut exchange).await;

        brkr.deposit_cash(&100_000.0);

        //No price for security so we haven't diffed correctly
        exchange.check().await;
        brkr.check().await;

        exchange.check().await;
        brkr.check().await;

        let mut target_weights = PortfolioAllocation::new();
        target_weights.insert("ABC", 0.9);

        let orders =
            BrokerCalculations::diff_brkr_against_target_weights(&target_weights, &mut brkr);
        brkr.send_orders(&orders).await;

        exchange.check().await;
        brkr.check().await;

        let orders1 =
            BrokerCalculations::diff_brkr_against_target_weights(&target_weights, &mut brkr);

        brkr.send_orders(&orders1).await;
        exchange.check().await;
        brkr.check().await;

        dbg!(brkr.get_position_qty("ABC"));
        //If the logic isn't correct the orders will have doubled up to 1800
        assert_eq!(*(*brkr.get_position_qty("ABC").unwrap()), 900.0);
    }

    #[tokio::test]
    async fn diff_handles_case_when_existing_order_requires_sell_to_rebalance() {
        //Tests similar scenario to previous test but for the situation in which the price is
        //missing, and we try to rebalance by buying but the pending order is for a significantly
        //greater amount of shares than we now need (e.g. we have a price of X, we miss a price,
        //and then it drops 20%).
        let clock = ClockBuilder::with_length_in_seconds(100, 5)
            .with_frequency(&Frequency::Second)
            .build();

        let mut price_source = HashMapPriceSource::new(clock.clone());
        price_source.add_quotes(100, Quote::new(100.00, 100.00, 100, "ABC"));
        price_source.add_quotes(103, Quote::new(75.00, 75.00, 103, "ABC"));
        price_source.add_quotes(104, Quote::new(75.00, 75.00, 104, "ABC"));

        let mut exchange = ConcurrentExchangeBuilder::new()
            .with_clock(clock.clone())
            .with_price_source(price_source)
            .build();

        let mut brkr: ConcurrentBroker<Dividend, HashMapCorporateEventsSource, Quote> =
            ConcurrentBrokerBuilder::new().build(&mut exchange).await;

        brkr.deposit_cash(&100_000.0);

        let mut target_weights = PortfolioAllocation::new();
        target_weights.insert("ABC", 0.9);
        let orders =
            BrokerCalculations::diff_brkr_against_target_weights(&target_weights, &mut brkr);
        println!("{:?}", orders);
        brkr.send_orders(&orders).await;

        //No price for security so we haven't diffed correctly
        exchange.check().await;
        brkr.check().await;

        exchange.check().await;
        brkr.check().await;

        exchange.check().await;
        brkr.check().await;

        let orders1 =
            BrokerCalculations::diff_brkr_against_target_weights(&target_weights, &mut brkr);
        println!("{:?}", orders1);

        brkr.send_orders(&orders1).await;

        exchange.check().await;
        brkr.check().await;

        println!("{:?}", brkr.get_holdings());
        //If the logic isn't correct then the order will be for less shares than is actually
        //required by the newest price
        assert_eq!(*(*brkr.get_position_qty("ABC").unwrap()), 1200.0);
    }
}
