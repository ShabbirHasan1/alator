mod common;

use std::collections::HashMap;
use std::rc::Rc;

use alator::broker::order::OrderExecutor;
use alator::broker::CashManager;
use alator::perf::PortfolioPerformance;
use alator::portfolio::{Portfolio, SimPortfolio};

#[test]
fn test_that_portfolio_creates_correct_orders_given_weights() {
    let (mut brkr, universe) = common::build_fake_data();
    brkr.deposit_cash(100_000.00);
    brkr.set_date(&100);

    let mut target_weights: HashMap<String, f64> = HashMap::new();
    target_weights.insert(String::from("ABC"), 0.5);
    target_weights.insert(String::from("BCD"), 0.5);

    let port = SimPortfolio::new(universe);
    let orders = port.update_weights(&target_weights, &brkr);
    for order in orders {
        match order.symbol.as_str() {
            "ABC" => assert!(order.shares == 490.0),
            "BCD" => assert!(order.shares == 99.0),
            _ => unreachable!("Shouldn't call with any other symbol"),
        }
    }
}

#[test]
fn test_that_portfolio_calculates_performance_accurately() {
    let mut perf = PortfolioPerformance::new();

    let (mut brkr, universe) = common::build_fake_data();
    brkr.deposit_cash(100_000.00);

    let mut target_weights: HashMap<String, f64> = HashMap::new();
    target_weights.insert(String::from("ABC"), 0.5);
    target_weights.insert(String::from("BCD"), 0.5);

    let port = SimPortfolio::new(Rc::clone(&universe));

    brkr.set_date(&100);
    let orders = port.update_weights(&target_weights, &brkr);
    brkr.execute_orders(orders);
    perf.update(&port, &brkr);

    brkr.set_date(&101);
    let orders = port.update_weights(&target_weights, &brkr);
    brkr.execute_orders(orders);
    perf.update(&port, &brkr);

    let portfolio_return = perf.get_portfolio_return();
    //We need to round up to cmp properly
    let to_comp = (portfolio_return * 100.0).round() as i64;
    assert!((to_comp as f64).eq(&69.0));
}
