source('src/streampulse/streampulse/domain_helpers.R')
library(StreamPULSE)
#retrieval kernels ####
## network = 'streampulse
## domain = 'streampulse'


#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {
  
  dis_sites <- c("EllerbeClub", "EllerbeGlenn", "Mud")
  
  prodname_ms <- "discharge__VERSIONLESS001"
  site_code <- "sitename_NA"
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code)
  
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)
  
  #streampulse_data_pipeline(dis_sites, raw_data_dest)
  
  component_1<- "streampulse_discharge_ellerbeclub"
  rawfile_1 <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component_1)
  
  component_2<- "streampulse_discharge_ellerbeglenn"
  rawfile_2 <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component_2)
  
  component_3<- "streampulse_discharge_mud"
  rawfile_3 <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component_3)
  
  #download data for sites with discharge
  discharge_res <- request_streampulse_data(dis_sites)
  
  #split into sites
  ellerbeclub_df <- discharge_res$EllerbeClub$data
  ellerbeglenn_df <- discharge_res$EllerbeGlenn$data
  mud_df <- discharge_res$Mud$data
  
  # download it to the raw file locatin
  write_csv(ellerbeclub_df, file = rawfile_1)
  write_csv(ellerbeglenn_df, file = rawfile_2)
  write_csv(mud_df, file = rawfile_3)
  
  res <- httr::HEAD(url)
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(url, '(requires authentication)'),
                    access_time = as.character(with_tz(Sys.time(),
                                                       tzone = 'UTC')),
                    last_mod_dt = last_mod_dt)
  
  return(deets_out)
  
}


#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {
  
  prodname_ms <- "stream_chemistry__VERSIONLESS002"
  site_code <- "sitename_NA"
  
  sites <- c("Ellerbe", "EllerbeCP", "EllerbeClub", "EllerbeGlenn", "EllerbeTrinity", "ColeMill", "Eno", "Mud", "NHC", "UNHC", "UEno", "Stony")
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code)
  
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)
  
  
  #create separate rawfile (file name), componenent, download data from streampulse, write csv
  streampulse_data_pipeline(sites, raw_data_dest)
  
  #call function for NC streampulse model results
  results_df <- request_streampulse_results(sites)
  
  Eno_res   <-results_df$Eno
  Mud_res   <-results_df$Mud
  NHC_res   <-results_df$NHC
  UNHC_res  <-results_df$UNHC
  UEno_res  <-results_df$UEno
  Stony_res <-results_df$Stony
  
  component_1<- "streampulse_stream_chemistry_EnoRes"
  rawfile_1 <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component_1)
  
  component_2<- "streampulse_stream_chemistry_MudRes"
  rawfile_2 <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component_2)
  
  component_3<- "streampulse_stream_chemistry_NHCRes"
  rawfile_3 <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component_3)
  
  component_4<- "streampulse_stream_chemistry_UNHCRes"
  rawfile_4 <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component_4)
  
  component_5<- "streampulse_stream_chemistry_UENORes"
  rawfile_5 <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component_5)
  component_6<- "streampulse_stream_chemistry_StonyRes"
  rawfile_6 <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component_6)
  write_csv(Eno_res, file = rawfile_1)
  write_csv(Mud_res, file = rawfile_2)
  write_csv(NHC_res, file = rawfile_3)
  write_csv(UNHC_res, file = rawfile_4)
  write_csv(UEno_res, file = rawfile_5)
  write_csv(Stony_res, file = rawfile_6)
  
  res <- httr::HEAD(url)
  
  
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(url, '(requires authentication)'),
                    access_time = as.character(with_tz(Sys.time(),
                                                       tzone = 'UTC')),
                    last_mod_dt = last_mod_dt)
  
  return(deets_out)
  
}


#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    prodname_ms <- "discharge__VERSIONLESS001"
    

    #rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
    #                n = network,
    #                d = domain,
    #                p = prodname_ms,
    #                s = site_code,
    #                c = component)
    
    rawfolder <- glue('data/{n}/{d}/raw/{p}/{s}',
                      n = network,
                      d = domain,
                      p = prodname_ms,
                      s = site_code)     
    
    
    
    all_files <- list.files(path = rawfolder, pattern = "*.csv", full.names = TRUE)
    
    
    list_of_dataframes <- map(all_files, ~read_delim(.x, delim = ','))
    
    
    d <- map(all_files, ~read_delim(.x, delim = ',') %>% as_tibble())

    d$'X_00060_00003'<-28.3168*(d$'X_00060_00003')
    
    
    
    #combine sites before this func
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                    datetime_cols = list('Date' = '%Y-%m-%d'),
                    datetime_tz = 'US/Central',
                    site_code_col = 'site_name',
                    data_cols = c('X_00060_00003'= 'discharge'),
                    data_col_pattern = '#V#',
                    is_sensor = TRUE)

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
    d <- qc_hdetlim_and_uncert(d, prodname_ms)
    d <- synchronize_timestep(d)
    
    sites <- unique(d$site_code)
    
    for(s in 1:length(sites)){
      d_site <- d %>%
        filter(site_code == !!sites[s])
      
      write_ms_file(d = d_site,
                    network = network,
                    domain = domain,
                    prodname_ms = prodname_ms,
                    site_code = sites[s],
                    level = 'munged', 
                    shapefile = FALSE)
    }
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {
  
  
  #rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
   #               n = network,
    #              d = domain,
     #             p = prodname_ms,
      #            s = site_code,
       #           c = component)
  
  prodname_ms <- "stream_chemistry__VERSIONLESSS002"
  rawfolder <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)     
  
  all_files <- list.files(path = rawfolder, pattern = "*.csv", full.names = TRUE)
  
  
  list_of_dataframes <- map(all_files, ~read_delim(.x, delim = ','))
  
  
  d <- map(all_files, ~read_delim(.x, delim = ',') %>% as_tibble())

  d$'X_00060_00003'<-28.3168*(d$'X_00060_00003')
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_name',
                       data_cols = c('X_00060_00003'= 'discharge'),
                       data_col_pattern = '#V#',
                       is_sensor = TRUE)

  d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
  d <- qc_hdetlim_and_uncert(d, prodname_ms)
  d <- synchronize_timestep(d)
  
  sites <- unique(d$site_code)
  
  for(s in 1:length(sites)){
    d_site <- d %>%
      filter(site_code == !!sites[s])
    
    write_ms_file(d = d_site,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_code = sites[s],
                  level = 'munged', 
                  shapefile = FALSE)
  }
}


#derive kernels ####

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- derive_stream_flux

