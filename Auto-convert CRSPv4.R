library(tidyverse)
library(gmailr)
library(tibbletime)
library(tidyquant)
library(clipr)
library(roll)
library(writexl)
library(twilio)
library(urlshorteneR)


#api_tiingo <- your_api_key
riingo::riingo_set_token(api_tiingo)

options(pillar.sigfig=5)


gm_auth_configure(path = "gmail_api.json")
gm_auth(cache = ".secret")
files_old <- list.files(path="P:/CRSP", pattern="*.txt", full.names=TRUE, recursive=FALSE)

previous_file_name <- "previous_file_name"
while (TRUE) {
  
  files_new <- list.files(path="P:/CRSP", pattern="*.txt", full.names=TRUE, recursive=FALSE)
  new_files <- files_new[!(files_new %in% files_old)]
  for (i in new_files){
    file_name <- i
    new_file_name <- paste(substr(file_name, 1, nchar(file_name)-4), ".csv", sep = "")
    new_file_name_summary <- paste(substr(file_name, 1, nchar(file_name)-4), "_summary.xlsx", sep = "")
    
    
    df = tibble(read.delim(file_name, header = FALSE, stringsAsFactors = FALSE, quote = "", sep = "|", skip = 1))
    colnames(df) <- df[1,]
    df <- df[-1,]
    if(nrow(df) > 1){
      df_fixed <- df %>%
        mutate(Effective_on_Open_Date = as.Date(paste(substr(Effective_on_Open_Date, 1, 4),"-",substr(Effective_on_Open_Date, 5,6), "-", 
                                                      substr(Effective_on_Open_Date, 7,8), sep = "")),
               Effective_After_Close_Date = as.Date(paste(substr(Effective_After_Close_Date, 1, 4),"-",substr(Effective_After_Close_Date, 5,6), "-", 
                                                          substr(Effective_After_Close_Date, 7,8), sep = "")),
               Holdings_Change = as.numeric(Holdings_Change)
               
               
        ) %>%
        relocate(Ticker, .before = Effective_on_Open_Date) %>%
        relocate(Holdings_Change, .after = Effective_After_Close_Date) %>%
        relocate(Index_Name, .after = Effective_After_Close_Date) %>%

        mutate(abs_holdings_change = abs(Holdings_Change)) %>%
        group_by(Ticker) %>%
        arrange(-abs_holdings_change)
      
      df_fixed <- df_fixed[,-1]
      df_fixed <- filter(df_fixed, !is.na(Effective_on_Open_Date))
      
      
      symbol_weights <- read_csv("P:/CRSP/CRSP_symbol_weight_03_05.csv")
      joined_list <- NULL
      
      for (ticker in unique(df_fixed$Ticker)){
        tick_filt <- filter(df_fixed, Ticker == ticker)
        for(index in unique(tick_filt$Index_Name)){
          index_filt <- filter(tick_filt, Index_Name == index)
          
          if(index == "CRSP US Total Market Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`total market`)
            
          } 
          else if(index == "CRSP US Mega Cap Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`mega`)
            
          }
          else if(index == "CRSP US Mega Cap Growth Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`mega growth`)
            
          } 
          else if(index == "CRSP US Mega Cap Value Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`mega value`)
            
          } 
          else if(index == "CRSP US Large Cap Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`large`)
            
          }
          else if(index == "CRSP US Large Cap Growth Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`large growth`)
            
          }
          else if(index == "CRSP US Large Cap value Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`large value`)
            
          }
          else if(index == "CRSP US Mid Cap Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`mid`)
            
          }
          else if(index == "CRSP US Mid Cap Growth Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`mid growth`)
            
          }
          else if(index == "CRSP US Mid Cap Value Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`mid value`)
            
          }
          else if(index == "CRSP US Small Cap Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`small`)
            
          }
          else if(index == "CRSP US Small Cap Growth Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`small growth`)
            
          }
          else if(index == "CRSP US Small Cap Value Index"){
            index_weight = as.numeric(filter(symbol_weights, Ticker == ticker)$`small value`)
          }
          else(index_weight = 0)
          
          if(length(index_weight) > 0){
            index_filt <- cbind(index_filt, index_weight)
          }
          if(length(index_weight) == 0){
            index_filt <- cbind(index_filt, 0)
          }
          joined_list <- rbind(joined_list, index_filt)
          
          
          
        }
        
      }
      colnames(joined_list)[length(colnames(joined_list))] <- "index_weight"
      
      joined_list[is.na(joined_list)] <- 0
      joined_list$Base_Holdings[which(joined_list$Base_Holdings == "")] <- 0
      joined_list$New_Holdings[which(joined_list$New_Holdings == "")] <- 0
      
      joined_list <- joined_list %>%
        group_by(Ticker) %>%
        summarise(max_holdings = max(as.numeric(Base_Holdings))) %>%
        inner_join(joined_list, by = "Ticker") %>%
        relocate(max_holdings, .after = Base_Holdings) %>%
        mutate(share_change = index_weight * Holdings_Change) %>%
        relocate(share_change, .after = Effective_After_Close_Date) %>%
        relocate(Holdings_Change, .after = share_change) %>%
        relocate(index_weight, .after = Index_Name)
      
      joined_list <- joined_list %>%
        mutate(current_shares = as.numeric(Base_Holdings) * index_weight) %>%
        mutate(new_shares = as.numeric(New_Holdings) * index_weight)%>%
        # mutate(new_shares = ifelse(is.na(new_shares), 0, new_shares)) %>%
        mutate(share_change = new_shares - current_shares) %>%
        # relocate(Event_Label, .after = Index_Name) %>%
        # relocate(Change_Description, .after = Event_Label) %>%
        mutate(abs_holdings_change = abs(share_change)) %>%
        ungroup() %>%
        group_by(Ticker, Effective_After_Close_Date) %>%
        arrange(Ticker, -abs_holdings_change) %>%
        select(Ticker, Effective_After_Close_Date, Index_Name, index_weight, current_shares, new_shares,
               share_change, Event_Label, Change_Description, Company_Name, Base_Holdings, Holdings_Change, New_Holdings, Base_Effective_Float_Factor,
               New_Effective_Float_Factor, Base_Effective_TSO, New_Size_Multiplier, New_Style_Multiplier, New_Concentration_Multiplier, New_Received_Stock_Multiplier, abs_holdings_change)
      
      
      
      iwf_weights <- read_csv("P:/CRSP/IWF_weights.csv")
      
      
      joined_list[is.na(joined_list)] <- 0
      joined_list$Base_Holdings[which(joined_list$Base_Holdings == "")] <- 0
      joined_list$New_Holdings[which(joined_list$New_Holdings == "")] <- 0
      
      
      no_weight_list <- filter(joined_list, index_weight == 0)
      
      
      for (i in 1:nrow(no_weight_list)){
        index <- no_weight_list$Index_Name[i]
        
        if(index == "CRSP US Total Market Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[2] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[2] %>%
              as.numeric()
          }
  
        } 
        else if(index == "CRSP US Mega Cap Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[3] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[3] %>%
              as.numeric()
          }
          
        }
        else if(index == "CRSP US Mega Cap Growth Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[4] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[4] %>%
              as.numeric()
          }
          
        } 
        else if(index == "CRSP US Mega Cap Value Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[5] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[5] %>%
              as.numeric()
          }
          
        } 
        else if(index == "CRSP US Large Cap Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[6] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[6] %>%
              as.numeric()
          }
          
        }
        else if(index == "CRSP US Large Cap Growth Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[7] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[7] %>%
              as.numeric()
          }
          
        }
        else if(index == "CRSP US Large Cap value Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[8] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[8] %>%
              as.numeric()
          }
          
        }
        else if(index == "CRSP US Mid Cap Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[9] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[9] %>%
              as.numeric()
          }
          
        }
        else if(index == "CRSP US Mid Cap Growth Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[10] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[10] %>%
              as.numeric()
          }
          
        }
        else if(index == "CRSP US Mid Cap Value Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[11] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[11] %>%
              as.numeric()
          }
          
        }
        else if(index == "CRSP US Small Cap Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[12] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[12] %>%
              as.numeric()
          }
          
        }
        else if(index == "CRSP US Small Cap Growth Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[13] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[13] %>%
              as.numeric()
          }
          
        }
        else if(index == "CRSP US Small Cap Value Index"){
          index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$Base_Effective_Float_Factor[i]))[14] %>%
            as.numeric()
          if(is.na(index_weight)){
            index_weight = filter(iwf_weights, as.numeric(IWF) == as.numeric(no_weight_list$New_Effective_Float_Factor[i]))[14] %>%
              as.numeric()
          }
        }
        else(index_weight = 0)
        
        
        no_weight_list$index_weight[i] <- index_weight
        
      }
      
      no_weight_list[is.na(no_weight_list)] <- 0
      
      no_weight_list <-  no_weight_list %>%
        mutate(current_shares = as.numeric(Base_Holdings) * index_weight) %>%
        mutate(new_shares = as.numeric(New_Holdings) * index_weight)%>%
        mutate(share_change = new_shares - current_shares)%>%
        mutate(is_index_weight_estimated = "Y")
      
      weighted_list <- filter(joined_list, index_weight != 0 ) %>%
        mutate(is_index_weight_estimated = "N")
      
      
      joined_list <- rbind(weighted_list, no_weight_list) %>%
        ungroup() %>%
        group_by(Ticker, Effective_After_Close_Date) %>%
        arrange(Effective_After_Close_Date, Ticker, -abs_holdings_change) %>%
        relocate(is_index_weight_estimated, .after = "Index_Name") %>%
        mutate(is_index_weight_estimated = ifelse(is_index_weight_estimated == "Y" & index_weight == 0, "N", is_index_weight_estimated)) %>%
        ungroup() %>%
        filter(Effective_After_Close_Date == max(Effective_After_Close_Date)) %>%
        group_by(Ticker, Effective_After_Close_Date)
      

      previous_file <- read_csv(previous_file_name)
    
      
      changes <- anti_join(joined_list %>% ungroup(), previous_file %>% ungroup(), by = c("Ticker", "Effective_After_Close_Date"))
      

      if(nrow(changes) > 0){
        

      share_change_summary <- changes %>%
        filter(Effective_After_Close_Date == Sys.Date()) %>%
        group_by(Ticker) %>%
        summarise(sum_share_change = sum(share_change)) %>%
        arrange(-sum_share_change)
      
      
      if(nrow(share_change_summary) > 0){
      daily_data <- tq_get(share_change_summary$Ticker,
                           get  = "tiingo",
                           from   =  today() - 7,
                           to     = today() + 1) %>%
        group_by(symbol) %>%
        filter(date == max(date)) %>%
        select(adjusted)
      
      share_change_summary <- full_join(share_change_summary, daily_data, by = c("Ticker" = "symbol"))
      
      colnames(share_change_summary) <- c("symbol", "share change", "last price")
      share_change_summary <-  share_change_summary %>%
        mutate(`dollar value` = `last price` * `share change`)
      
      
      event_subset <- unique(joined_list %>% select(Ticker, 9, 10)) %>% filter(Effective_After_Close_Date == max(Effective_After_Close_Date))
      share_change_summary <-   inner_join(share_change_summary, event_subset, by = c("symbol" =  "Ticker"))
      
      colnames(share_change_summary) <- c("symbol", "share change", "last price", "dollar value", "effective date", "event label", "change description")

      
      
      write_csv(joined_list, new_file_name)
      write_xlsx(share_change_summary, new_file_name_summary)
      
      email_list <- c("email1", "email2")
      to_phone_num <- "#####"
      from_phone num <- "######"
      
        test_email <-
          gm_mime() %>%
          gm_to(email_list) %>%
          gm_from("kosachevskyym@gmail.com") %>%
          gm_subject(paste("CRSP Index Changes", as.character(Sys.time()))) %>%
          gm_text_body("CRSP Daily Index Changes") %>%
          gm_attach_file(new_file_name) %>%
          gm_attach_file(new_file_name_summary)
        
        #try sending an email. If error, it'll send me an error text
        tryCatch(
          #try to do this
          { 
            gm_send_message(test_email)
          },
          #if an error occurs, tell me the error
          error=function(e) {
            tw_send_message(
              to = to_phone_num,
              from = from_phone,
              body = "CRSP FWD Error!"
              
            ) 
          }
        ) 


        #if no error, it'll send me a successful text 
        tw_send_message(
          to = to_phone_num,
          from = from_phone,
          body = "New CRSP Email"
          
        ) 
        
        
        previous_file_name <- new_file_name
        ##
      } 
      }  
    }}
  
  files_old <- files_new
  Sys.sleep(30)
  
}
