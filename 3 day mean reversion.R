#This code runs a screener for a trading strategy
#The strategy looks to find stocks that have had high standard deviation moves over the last 3 days and revert them
# I use tiingo for my historical and intraday data source



library(tibbletime)
library(tidyverse)
library(tidyquant)
library(roll)
library(riingo)


#Import a list of S&P 500 members or any sufficienctly liquid list of stocks
stock_list <- read_csv("spx.csv")


stocks_2022 <- stock_list$symbol
win_loss_table <- stats_table <- full_data <- short_portfolio_full <- long_portfolio_full <- NULL

#get historical data
daily_data <- tq_get(stocks_2022,
                     get  = "tiingo",
                     from = today() - 50,
                     to   =  today() - 1)

daily_data <- daily_data %>%
  select(symbol, date, adjOpen, adjusted) %>%
  mutate(date = as.Date(date))

colnames(daily_data) <- c("ticker", "date", "open", "last")
last_date <- daily_data %>%
  arrange(date) %>%
  select(date) %>%
  unique() %>%
  tail(1)


tickers <- daily_data %>%
  filter(date == last_date$date)

tickers <- unique(tickers$ticker)


#I've found the mean reversion is best implemented before noon each day. The precise timing is discretionary
while(as.POSIXct(Sys.time(), format = "%H:%M:%S") <= as.POSIXct("12:00:00", format = "%H:%M:%S")){
  
  #Get live data here
  live_data <- riingo_iex_quote(tickers)
  
  current_price <- live_data %>%
    select(ticker, last) %>%
    mutate(date = today()) %>%
    mutate(open = last)
  
  #Append the live data point to the historical data
  all_data <- rbind(daily_data, current_price)
  
  #define the number of securities to be used in a long and short portfolio
  long_num_securities <- short_num_securities <- 15
  
  #Calculate the average change, std of change, and z score of change for the dataset
  step_one <- all_data %>%
    ungroup() %>%
    group_by(ticker) %>%
    arrange(ticker, date) %>%
    mutate(
      mov_avg = roll_mean(lag(open), 25),
      change_3d = (open  - lag(open, 3)) / lag(open, 3),
      roll_std_3d = roll_sd(lag(change_3d), 25),
      roll_mean_3d = roll_mean(lag(change_3d), 25),
      z_score= (change_3d - roll_mean_3d) / roll_std_3d
      
    ) %>%
    filter(!is.na(mov_avg)) %>%
    filter(open > mov_avg) %>%
    ungroup() %>%
    group_by(date) %>%
    arrange(date) 

  
  #Short portfolio will be composed of stocks that have had a high positive z score of change in the last 3 days, 
  #and that have opened higher than the previous close
  
  short_portfolio <- step_one %>%
    filter(z_score > 0) %>%
    filter(open > lag(last)) %>%
    group_by(date) %>%
    arrange(date, -z_score) %>%
    slice(1:short_num_securities) %>%
    filter(date == today())

  #Long  portfolio will be composed of stocks that have had a high negative z score of change in the last 3 days, 
  #and that have opened lower  than the previous close
  
  long_portfolio <- step_one %>%
    filter(z_score < 0) %>%
    filter(open < lag(last)) %>%
    group_by(date) %>%
    arrange(date, z_score) %>%
    slice(1:long_num_securities) %>%
    filter(date == today())
  
  short_portfolio <- inner_join(short_portfolio, stock_list, by = c("ticker" = "symbol"))
  long_portfolio <- inner_join(long_portfolio, stock_list, by = c("ticker" = "symbol"))
  
  print(short_portfolio)
  print(long_portfolio)
  
  write_csv(long_portfolio)
  write_csv(short_portfolio)
  
  
  #repeat this check ever 2 min 30 sec
  sleep_time <- 2.5 * 60
  Sys.sleep(sleep_time)
  
}
