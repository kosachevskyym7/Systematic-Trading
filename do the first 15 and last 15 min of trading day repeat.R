library(jsonlite)
library(tibbletime)
library(tidyverse)
library(tidyquant)
library(clipr)
library(roll)


#Use tiingo API for intraday data
#This code tests whether a significant (read: high sd) move in the first 15 min, followed by a reversion in the opposite direction throughout the day
#usually revert again in the last 15 min of the trading day

options(pillar.sigfig=5)

#full_data is the intraday data I obtained from tiingo in a different code chunk
intraday_data <- full_data %>%
  mutate(short_date = date(date))

#filter data to the first 30 min
morning_subset <- intraday_data %>%
  group_by(symbol, short_date) %>%
  filter(date < as.POSIXct(paste(short_date, "10:00:00"))) %>%
  mutate(morning_change = (close - open)/ close) %>%
  filter(!is.na(morning_change))

#filter data to the last 30 minutes
evening_subset <- intraday_data %>%
  group_by(symbol, short_date) %>%
  filter(date > as.POSIXct(paste(short_date, "15:30:00"))) %>%
  mutate(evening_change = (close - open)/ close) %>%
  filter(!is.na(evening_change))


morning_select <- morning_subset %>%
  select(symbol, date, open, close, morning_change)

evening_select <- evening_subset %>%
  select(symbol, date, open, close, evening_change)

#join the morning and evening dataset on symbol and date and then calculate absolute change in the first 30 and last 30 min
m_e <- inner_join(morning_select, evening_select, by = c("short_date", "symbol"), suffix = c("_morn", "_even")) %>%
  select(short_date, open_morn, open_even, morning_change, evening_change) %>%
  mutate(abs_morn_change = abs(morning_change), 
         abs_even_change = abs(evening_change))

#caluclate rolling mean, sd to then calculate the z score for any given day
#calculate a reverse column which indicates whether a stock reversed back to above or below the opening print after its move in the morning
#filter for high sd moves that were followed by this reversal

m_e_mean <- m_e %>%
  ungroup() %>%
  group_by(symbol) %>%
  mutate(roll_mean = roll_mean(abs_morn_change, width = 50), 
         roll_sd = roll_sd(abs_morn_change, width = 50)) %>%
  filter(!is.na(roll_mean)) %>%
  mutate(z = (abs_morn_change - roll_mean) / roll_sd ) %>%
  mutate(reverse = ifelse(
    (morning_change > 0 & open_even < open_morn) | (morning_change < 0 & open_even > open_morn), 1, 0
  )) %>%
  filter(z > 1, reverse == 1) %>%
  mutate(return = ifelse(
    morning_change > 0, evening_change, -evening_change
  ))

#group by symbol and calculate a few basic statistics
stats_ticker <- m_e_mean %>%
  ungroup() %>%
  group_by(symbol) %>%
  summarise(mean = mean(return),
            sd = sd(return),
            median = median(return),
            n = n(),
            win_rate = sum(return > 0) / n()) %>%
  mutate(sharpe = mean / sd) %>%
  arrange(-sharpe)




