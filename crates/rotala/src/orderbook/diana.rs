use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

// Unclear if the right approach is traits but this was the quickest way
pub trait DianaSource {
    fn get_quote(&self, date: &i64, security: &str) -> Option<impl DianaQuote>;
}

pub trait DianaQuote {
    fn get_ask(&self) -> f64;
    fn get_bid(&self) -> f64;
}

pub type DianaOrderId = u64;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum DianaTradeType {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum DianaOrderType {
    MarketSell,
    MarketBuy,
    LimitSell,
    LimitBuy,
    StopSell,
    StopBuy,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DianaTrade {
    pub symbol: String,
    pub value: f64,
    pub quantity: f64,
    pub date: i64,
    pub typ: DianaTradeType,
}

impl DianaTrade {
    pub fn new(
        symbol: impl Into<String>,
        value: f64,
        quantity: f64,
        date: i64,
        typ: DianaTradeType,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            value,
            quantity,
            date,
            typ,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DianaOrder {
    pub order_id: Option<DianaOrderId>,
    pub order_type: DianaOrderType,
    pub symbol: String,
    pub shares: f64,
    pub price: Option<f64>,
}

impl DianaOrder {
    pub fn get_shares(&self) -> f64 {
        self.shares
    }

    pub fn get_symbol(&self) -> &str {
        &self.symbol
    }
    pub fn get_price(&self) -> &Option<f64> {
        &self.price
    }

    pub fn get_order_type(&self) -> &DianaOrderType {
        &self.order_type
    }
}

impl Eq for DianaOrder {}

impl PartialEq for DianaOrder {
    fn eq(&self, other: &Self) -> bool {
        self.symbol == other.symbol
            && self.order_type == other.order_type
            && self.shares == other.shares
    }
}

impl DianaOrder {
    fn set_order_id(&mut self, order_id: u64) {
        self.order_id = Some(order_id);
    }

    fn market(order_type: DianaOrderType, symbol: impl Into<String>, shares: f64) -> Self {
        Self {
            order_id: None,
            order_type,
            symbol: symbol.into(),
            shares,
            price: None,
        }
    }

    fn delayed(
        order_type: DianaOrderType,
        symbol: impl Into<String>,
        shares: f64,
        price: f64,
    ) -> Self {
        Self {
            order_id: None,
            order_type,
            symbol: symbol.into(),
            shares,
            price: Some(price),
        }
    }

    pub fn market_buy(symbol: impl Into<String>, shares: f64) -> Self {
        DianaOrder::market(DianaOrderType::MarketBuy, symbol, shares)
    }

    pub fn market_sell(symbol: impl Into<String>, shares: f64) -> Self {
        DianaOrder::market(DianaOrderType::MarketSell, symbol, shares)
    }

    pub fn stop_buy(symbol: impl Into<String>, shares: f64, price: f64) -> Self {
        DianaOrder::delayed(DianaOrderType::StopBuy, symbol, shares, price)
    }

    pub fn stop_sell(symbol: impl Into<String>, shares: f64, price: f64) -> Self {
        DianaOrder::delayed(DianaOrderType::StopSell, symbol, shares, price)
    }

    pub fn limit_buy(symbol: impl Into<String>, shares: f64, price: f64) -> Self {
        DianaOrder::delayed(DianaOrderType::LimitBuy, symbol, shares, price)
    }

    pub fn limit_sell(symbol: impl Into<String>, shares: f64, price: f64) -> Self {
        DianaOrder::delayed(DianaOrderType::LimitSell, symbol, shares, price)
    }
}

#[derive(Debug)]
pub struct Diana {
    inner: VecDeque<DianaOrder>,
    last_inserted: u64,
}

impl Default for Diana {
    fn default() -> Self {
        Self::new()
    }
}

impl Diana {
    pub fn new() -> Self {
        Self {
            inner: std::collections::VecDeque::new(),
            last_inserted: 0,
        }
    }

    pub fn delete_order(&mut self, delete_order_id: u64) {
        let mut delete_position: Option<usize> = None;
        for (position, order) in self.inner.iter().enumerate() {
            if let Some(order_id) = order.order_id {
                if order_id == delete_order_id {
                    delete_position = Some(position);
                    break;
                }
            }
        }
        if let Some(position) = delete_position {
            self.inner.remove(position);
        }
    }

    pub fn insert_order(&mut self, order: &mut DianaOrder) {
        order.set_order_id(self.last_inserted);
        self.inner.push_back(order.clone());
        self.last_inserted += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn execute_buy(quote: impl DianaQuote, order: &DianaOrder, date: i64) -> DianaTrade {
        let trade_price = quote.get_ask();
        let value = trade_price * order.get_shares();
        DianaTrade {
            symbol: order.get_symbol().to_string(),
            value,
            quantity: order.get_shares(),
            date,
            typ: DianaTradeType::Buy,
        }
    }

    fn execute_sell(quote: impl DianaQuote, order: &DianaOrder, date: i64) -> DianaTrade {
        let trade_price = quote.get_bid();
        let value = trade_price * order.get_shares();
        DianaTrade {
            symbol: order.get_symbol().to_string(),
            value,
            quantity: order.get_shares(),
            date,
            typ: DianaTradeType::Sell,
        }
    }

    pub fn execute_orders(&mut self, date: i64, source: &impl DianaSource) -> Vec<DianaTrade> {
        let mut completed_orderids = Vec::new();
        let mut trade_results = Vec::new();
        if self.is_empty() {
            return trade_results;
        }

        for order in self.inner.iter() {
            let security_id = &order.symbol;
            if let Some(quote) = source.get_quote(&date, security_id) {
                let result = match order.order_type {
                    DianaOrderType::MarketBuy => Some(Self::execute_buy(quote, order, date)),
                    DianaOrderType::MarketSell => Some(Self::execute_sell(quote, order, date)),
                    DianaOrderType::LimitBuy => {
                        //Unwrap is safe because LimitBuy will always have a price
                        let order_price = order.price;
                        if order_price >= Some(quote.get_ask()) {
                            Some(Self::execute_buy(quote, order, date))
                        } else {
                            None
                        }
                    }
                    DianaOrderType::LimitSell => {
                        //Unwrap is safe because LimitSell will always have a price
                        let order_price = order.price;
                        if order_price <= Some(quote.get_bid()) {
                            Some(Self::execute_sell(quote, order, date))
                        } else {
                            None
                        }
                    }
                    DianaOrderType::StopBuy => {
                        //Unwrap is safe because StopBuy will always have a price
                        let order_price = order.price;
                        if order_price <= Some(quote.get_ask()) {
                            Some(Self::execute_buy(quote, order, date))
                        } else {
                            None
                        }
                    }
                    DianaOrderType::StopSell => {
                        //Unwrap is safe because StopSell will always have a price
                        let order_price = order.price;
                        if order_price >= Some(quote.get_bid()) {
                            Some(Self::execute_sell(quote, order, date))
                        } else {
                            None
                        }
                    }
                };
                if let Some(trade) = &result {
                    completed_orderids.push(order.order_id.unwrap());
                    trade_results.push(trade.clone());
                }
            }
        }
        for order_id in completed_orderids {
            self.delete_order(order_id);
        }
        trade_results
    }
}

#[cfg(test)]
mod tests {
    use super::Diana as OrderBook;
    use super::DianaOrder;
    use crate::clock::{Clock, Frequency};
    use crate::input::penelope::Penelope;
    use crate::input::penelope::PenelopeBuilder;
    use crate::orderbook::diana::DianaOrderType;

    fn setup() -> (Clock, Penelope) {
        let mut price_source_builder = PenelopeBuilder::new();
        price_source_builder.add_quote(101.0, 102.00, 100, "ABC".to_string());
        price_source_builder.add_quote(102.0, 103.00, 101, "ABC".to_string());
        price_source_builder.add_quote(105.0, 106.00, 102, "ABC".to_string());

        let (penelope, clock) = price_source_builder.build_with_frequency(Frequency::Second);
        (clock, penelope)
    }

    #[test]
    fn test_that_multiple_orders_will_execute() {
        let (_clock, source) = setup();
        let mut orderbook = OrderBook::new();
        let mut order = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::MarketBuy,
            symbol: "ABC".to_string(),
            shares: 25.0,
            price: None,
        };
        orderbook.insert_order(&mut order);
        let mut order1 = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::MarketBuy,
            symbol: "ABC".to_string(),
            shares: 25.0,
            price: None,
        };
        orderbook.insert_order(&mut order1);
        let mut order2 = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::MarketBuy,
            symbol: "ABC".to_string(),
            shares: 25.0,
            price: None,
        };
        orderbook.insert_order(&mut order2);
        let mut order3 = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::MarketBuy,
            symbol: "ABC".to_string(),
            shares: 25.0,
            price: None,
        };
        orderbook.insert_order(&mut order3);

        let executed = orderbook.execute_orders(100.into(), &source);
        assert_eq!(executed.len(), 4);
    }

    #[test]
    fn test_that_buy_market_executes() {
        let (_clock, source) = setup();
        let mut orderbook = OrderBook::new();
        let mut order = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::MarketBuy,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: None,
        };

        orderbook.insert_order(&mut order);
        let mut executed = orderbook.execute_orders(100.into(), &source);
        assert_eq!(executed.len(), 1);

        let trade = executed.pop().unwrap();
        //Trade executes at 100 so trade price should be 102
        assert_eq!(trade.value / trade.quantity, 102.00);
        assert_eq!(trade.date, 100);
    }

    #[test]
    fn test_that_sell_market_executes() {
        let (_clock, source) = setup();
        let mut orderbook = OrderBook::new();
        let mut order = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::MarketSell,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: None,
        };

        orderbook.insert_order(&mut order);
        let mut executed = orderbook.execute_orders(100.into(), &source);
        assert_eq!(executed.len(), 1);

        let trade = executed.pop().unwrap();
        //Trade executes at 100 so trade price should be 101
        assert_eq!(trade.value / trade.quantity, 101.00);
        assert_eq!(trade.date, 100);
    }

    #[test]
    fn test_that_buy_limit_triggers_correctly() {
        let (_clock, source) = setup();
        let mut orderbook = OrderBook::new();
        let mut order = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::LimitBuy,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: Some(95.0),
        };
        let mut order1 = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::LimitBuy,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: Some(105.0),
        };

        orderbook.insert_order(&mut order);
        orderbook.insert_order(&mut order1);
        let mut executed = orderbook.execute_orders(100.into(), &source);
        //Only one order should execute on this tick
        assert_eq!(executed.len(), 1);

        let trade = executed.pop().unwrap();
        //Limit order has price of 105 but should execute at the ask, which is 102
        assert_eq!(trade.value / trade.quantity, 102.00);
        assert_eq!(trade.date, 100);
    }

    #[test]
    fn test_that_sell_limit_triggers_correctly() {
        let (_clock, source) = setup();
        let mut orderbook = OrderBook::new();
        let mut order = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::LimitSell,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: Some(95.0),
        };
        let mut order1 = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::LimitSell,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: Some(105.0),
        };

        orderbook.insert_order(&mut order);
        orderbook.insert_order(&mut order1);
        let mut executed = orderbook.execute_orders(100.into(), &source);
        //Only one order should execute on this tick
        assert_eq!(executed.len(), 1);

        let trade = executed.pop().unwrap();
        //Limit order has price of 95 but should execute at the ask, which is 101
        assert_eq!(trade.value / trade.quantity, 101.00);
        assert_eq!(trade.date, 100);
    }

    #[test]
    fn test_that_buy_stop_triggers_correctly() {
        //We are short from 90, and we put a StopBuy of 95 & 105 to take
        //off the position. If we are quoted 101/102 then 95 order
        //should be executed.

        let (_clock, source) = setup();
        let mut orderbook = OrderBook::new();
        let mut order = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::StopBuy,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: Some(95.0),
        };
        let mut order1 = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::StopBuy,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: Some(105.0),
        };

        orderbook.insert_order(&mut order);
        orderbook.insert_order(&mut order1);
        let mut executed = orderbook.execute_orders(100.into(), &source);
        //Only one order should execute on this tick
        assert_eq!(executed.len(), 1);

        let trade = executed.pop().unwrap();
        //Stop order has price of 103 but should execute at the ask, which is 102
        assert_eq!(trade.value / trade.quantity, 102.00);
        assert_eq!(trade.date, 100);
    }

    #[test]
    fn test_that_sell_stop_triggers_correctly() {
        //Long from 110, we place orders to exit at 100 and 105.
        //If we are quoted 101/102 then our 105 StopSell is executed.

        let (_clock, source) = setup();
        let mut orderbook = OrderBook::new();
        let mut order = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::StopSell,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: Some(99.0),
        };
        let mut order1 = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::StopSell,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: Some(105.0),
        };

        orderbook.insert_order(&mut order);
        orderbook.insert_order(&mut order1);
        let mut executed = orderbook.execute_orders(100.into(), &source);
        //Only one order should execute on this tick
        assert_eq!(executed.len(), 1);

        let trade = executed.pop().unwrap();
        //Stop order has price of 105 but should execute at the bid, which is 101
        assert_eq!(trade.value / trade.quantity, 101.00);
        assert_eq!(trade.date, 100);
    }

    #[test]
    fn test_that_order_for_nonexistent_stock_fails_silently() {
        let (_clock, source) = setup();
        let mut orderbook = OrderBook::new();
        let mut order = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::MarketBuy,
            symbol: "XYZ".to_string(),
            shares: 100.0,
            price: None,
        };

        orderbook.insert_order(&mut order);
        let executed = orderbook.execute_orders(100.into(), &source);
        assert_eq!(executed.len(), 0);
    }

    #[test]
    fn test_that_order_with_missing_price_executes_later() {
        let mut price_source_builder = PenelopeBuilder::new();
        price_source_builder.add_quote(101.00, 102.00, 100, "ABC".to_string());
        price_source_builder.add_quote(105.00, 106.00, 102, "ABC".to_string());

        let (price_source, mut clock) =
            price_source_builder.build_with_frequency(Frequency::Second);

        clock.tick();

        let mut orderbook = OrderBook::new();
        let mut order = DianaOrder {
            order_id: None,
            order_type: DianaOrderType::MarketBuy,
            symbol: "ABC".to_string(),
            shares: 100.0,
            price: None,
        };
        orderbook.insert_order(&mut order);
        let orders = orderbook.execute_orders(101.into(), &price_source);
        //Trades cannot execute without prices
        assert_eq!(orders.len(), 0);
        assert!(!orderbook.is_empty());

        clock.tick();
        //Order executes now with prices
        let mut orders = orderbook.execute_orders(102.into(), &price_source);
        assert_eq!(orders.len(), 1);

        let trade = orders.pop().unwrap();
        assert_eq!(trade.value / trade.quantity, 106.00);
        assert_eq!(trade.date, 102);
    }
}