# %%
import pandas as pd

# Read the Parquet file into a DataFrame
df = pd.read_parquet("clustering_results.parquet")

# Display the first few rows
print(df.head(5))

# %%
print(f"Min date {df["Datetime Event Began"].min()}")
# 2023-01-02 18:22:00
# %%
print(f"Max date {df["Datetime Event Began"].max()}")
# 2023-11-27 06:00:00
# %%

# event_id, state_event, Datetime Event Began, Datetime Restoration,
# Event Type,  fips,    state,   county, start_time,  duration,
# end_time, min_customers,  max_customers,  mean_customers,  cluster  


# event_id state_event Datetime Event Began Datetime Restoration  \
# 0  Alabama-0     Alabama  2023-01-12 14:00:00  2023-01-13 03:00:00   
# 1  Alabama-0     Alabama  2023-01-12 14:00:00  2023-01-13 03:00:00   
# 2  Alabama-0     Alabama  2023-01-12 14:00:00  2023-01-13 03:00:00   
# 3  Alabama-0     Alabama  2023-01-12 14:00:00  2023-01-13 03:00:00   
# 4  Alabama-0     Alabama  2023-01-12 14:00:00  2023-01-13 03:00:00   

#        Event Type  fips    state   county           start_time  duration  \
# 0  Severe Weather  1001  Alabama  Autauga  2023-01-12 19:00:00     52.00   
# 1  Severe Weather  1003  Alabama  Baldwin  2023-01-12 15:45:00      1.25   
# 2  Severe Weather  1003  Alabama  Baldwin  2023-01-12 19:15:00      3.00   
# 3  Severe Weather  1007  Alabama     Bibb  2023-01-13 04:00:00      7.25   
# 4  Severe Weather  1009  Alabama   Blount  2023-01-12 16:30:00      3.50   

#               end_time  min_customers  max_customers  mean_customers  cluster  
# 0  2023-01-14 23:00:00            216           6873     1259.716346        1  
# 1  2023-01-12 17:00:00            239            716      340.800000        1  
# 2  2023-01-12 22:15:00            251           1107      557.916667        1  
# 3  2023-01-13 11:15:00            430            525      448.827586        1  
# 4  2023-01-12 20:00:00            204            778      413.571429        1