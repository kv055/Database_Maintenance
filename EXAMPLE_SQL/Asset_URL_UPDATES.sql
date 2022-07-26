select * from new_urls;
select * from kraken_assets;

select count(*) from new_urls
left join kraken_assets on new_urls.ticker = kraken_assets.ticker
where kraken_assets.live_data_url <> new_urls.live_data_url 
	or kraken_assets.id is null;

# New or changed data
insert into kraken_assets
	select 
    null, new_urls.ticker, new_urls.historical_data_url, new_urls.live_data_url 
    from new_urls
	left join kraken_assets on new_urls.ticker = kraken_assets.ticker
	where kraken_assets.live_data_url <> new_urls.live_data_url 
		or kraken_assets.id is null ;
        
select count(*) from new_urls
left join kraken_assets on new_urls.ticker = kraken_assets.ticker
where kraken_assets.live_data_url <> new_urls.live_data_url 
	or kraken_assets.id is null;


# old / de-listed:
select * from new_urls
right join kraken_assets on new_urls.ticker = kraken_assets.ticker
where new_urls.id is null;
    
    