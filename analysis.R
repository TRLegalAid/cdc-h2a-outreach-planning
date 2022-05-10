# Produce spreadsheet with total number of H-2A workers for active jobs for each county for each month in 2021

# connect to db
library(DBI)
library(RPostgres)
library(dbplyr)
library(pool)

library(tidyverse)
library(lubridate)
library(readxl)
library(tigris)
library(sf)
library(janitor)


# pull database credentials from config.yml
dw <- config::get("db")

#create the connection to the database

con <- dbPool(RPostgres::Postgres(),
              dbname = 'df0jos6ceujrkb',
              host = dw$server,
              port = dw$port,
              user = dw$uid,
              password = dw$pwd)


# Pull H2A jobs with primary housing in Texas that ended on or after Jan. 1 2021 and began on or before Dec. 31 2021
h2a <- dbGetQuery(con,
                  'select * from job_central where "HOUSING_STATE" = \'Texas\'
                      and "Visa type" = \'H-2A\'
                      and "EMPLOYMENT_END_DATE" >= \'2021-01-01\'
                      and "EMPLOYMENT_BEGIN_DATE" <= \'2021-12-31\'')



#-------- Clean up to prepare for analysis

# Add fields for begin and end months; filter out withdrawn and denied job orders
# For the employment end month variable, subtract one if the DAY the job ended was the 1st or 2nd day of the month.
# This means we won't count that job as active for that month, and the workers won't be included in the totals. 
# We do this only for end date because many jobs end on the 1st of the month, but not many begin at the end of the month.
h2a_clean <- h2a %>% mutate(
  EMPLOYMENT_BEGIN_MONTH = ifelse(year(EMPLOYMENT_BEGIN_DATE) < 2021, 0, month(EMPLOYMENT_BEGIN_DATE)),
  EMPLOYMENT_END_MONTH = case_when(
    year(EMPLOYMENT_END_DATE) > 2021 ~ 13,
    day(EMPLOYMENT_END_DATE) < 3 ~ (month(EMPLOYMENT_END_DATE) - 1), # About 1/3 end on the 1st of the month; we're excluding these
    TRUE ~ month(EMPLOYMENT_END_DATE)
  )
) %>% filter(!str_detect(CASE_STATUS, 'Withdrawn') & !str_detect(CASE_STATUS, 'Denied'))



#-------- Expand df so we have a row for every month every job is active

h2a_expanded <- do.call(rbind, apply(h2a_clean, 1, function(x) 
  data.frame(CASE_NUMBER = x[30], 
             CASE_STATUS = x[31],
             EMPLOYMENT_BEGIN_DATE = x[72],
             EMPLOYMENT_BEGIN_MONTH = x[202],
             EMPLOYMENT_END_DATE = x[73],
             EMPLOYMENT_END_MONTH = x[203],
             HOUSING_COUNTY = x[91],
             HOUSING_STATE = x[93],
             WORKSITE_COUNTY = x[177],
             WORKSITE_STATE = x[179],
             TOTAL_WORKERS_NEEDED = x[160],
             TOTAL_WORKERS_H2A_CERTIFIED =x[158],
             month = seq(x[202], x[203], by=1)))) %>%
  
            # Need total workers as a numeric value
            mutate(TOTAL_WORKERS_NEEDED_N = as.numeric(TOTAL_WORKERS_NEEDED), 
                   TOTAL_WORKERS_H2A_CERTIFIED_N = as.numeric(TOTAL_WORKERS_H2A_CERTIFIED),
                   HOUSING_COUNTY_CLEAN = str_to_upper(HOUSING_COUNTY)) %>%
  
            # Filter out month = 0 and month = 13 (Start date in 2020 and end date in 2022)
            filter(month != 0 & month != 13)



#--------- Sum workers by county and month
h2a_county_month <- h2a_expanded %>% group_by(HOUSING_COUNTY_CLEAN, month) %>% 
  summarise(sum_total_workers_certified = sum(TOTAL_WORKERS_H2A_CERTIFIED_N)) 


# Table to share with FW outreach team:
h2a_wide <- h2a_county_month %>% 
  pivot_wider(names_from=month, values_from=sum_total_workers_certified) %>% 
  ungroup() %>%
  mutate_all(~replace_na(., 0))

colnames(h2a_wide) <- c("HOUSING_COUNTY", "January", "February", "March", "April", "May", "June", "July", "August", 
                             "September", "October", "November", "December")

# write.csv(h2a_wide, 'data/interim/2021_h2a_workers_certified_by_month.csv')





#---------- Load county boundaries

# Load from .geojson file
counties <- st_read('data/tx_counties.geojson') %>% 
  mutate(NAME = str_to_upper(CountyName)) %>% 
  select(OBJECTID, NAME, geometry)

# join fw data
counties_fw <- geo_join(counties, h2a_wide, 
                        by_df="HOUSING_COUNTY_CLEAN", 
                        by_sp="NAME")


# convert to centroid points

counties_ctds <- st_centroid(counties_fw) %>% mutate_all(~replace_na(., 0))

colnames(counties_ctds) <- c("OBJECTID", "NAME", "January", "February", "March", "April", "May", "June", "July", "August", 
                             "September", "October", "November", "December", "rank", "geometry")

# st_write(counties_fw, '2021_fw_pop_by_county.geojson')
# st_write(counties_ctds, '2021_fw_pop_by_county_pts.geojson')




# ---------- Sample map:

ggplot() +
  geom_sf(data = counties, fill="grey80") +
  geom_sf(data = counties_ctds, 
          pch = 21,
          aes(size = February),
          fill = alpha("purple", 0.7),
          col = "grey20") +
  scale_size(range = c(1,9))



# ---------- Sample charts:

h2a_dallam <- h2a_county_month %>% filter(HOUSING_COUNTY_CLEAN == 'DALLAM')

ggplot(h2a_dallam, aes(x=month, y=sum_total_workers)) +
  geom_bar(stat = "identity") +
  scale_x_continuous(breaks = round(seq(min(h2a_dallam$month), max(h2a_dallam$month), by =1),1))


h2a_county_month %>% ggplot(aes(x=month, y =sum_total_workers)) +
  geom_bar(stat = "identity") +
  labs(title = "Farmworker Population by Month",
       subtitle = "Number of workers requested for jobs active by month in 2021",
       x = element_blank(),
       y = element_blank()) +
  scale_x_continuous(breaks = round(seq(min(h2a_county_month$month), max(h2a_county_month$month), by=1),1))




# ---- Load vaccination rate data

vr <- read_excel('data/raw/TX VAX Counties 05.02.2022.xlsx', sheet=2) %>% clean_names()

vr_rates <- vr %>% select(county, farmworker_towns, x5_2019_population_estimate, fully_vaccinated, percent_of_fully_vaccinated)

# left join vaccination rate data to FW data

h2a_wide_vr <- left_join(h2a_wide, vr_rates, by = c('HOUSING_COUNTY' = 'county')) %>% 
  mutate(
    below_50_pct = case_when(
      percent_of_fully_vaccinated <= 0.5 ~ 1,
      TRUE ~ 0
    )
  )


# write.csv(h2a_wide_vr, 'data/interim/2021_h2a_workers_certified_by_month_vr.csv')

