create or replace task NBA_MONTE_CARLO.PIPELINE.HOURLY_SIMULATION
	warehouse=NBA_MONTE_CARLO_WH
	schedule='USING CRON 0 */4 * * * America/Los_Angeles'
	as EXECUTE dbt project NBA_SIMULATION args='build --target dev';
