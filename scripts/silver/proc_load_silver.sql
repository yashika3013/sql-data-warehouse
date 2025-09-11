Create or alter procedure silver.load_silver as
begin
	declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	begin try
		SET @batch_start_time = GETDATE();
		Print '==============================================';
		Print 'Loading Silver Layer';
		Print '==============================================';

		Print '----------------------------------------------';
		Print 'Loading CRM Tables';
		Print '----------------------------------------------';		Print '>> Truncating table silver.crm_cust_info';
		
		--loading silver.crm_cust_info
		SET @start_time = GETDATE()
		Print '>> Truncating table silver.crm_cust_info';
		Truncate table silver.crm_cust_info
		Print '>> Inserting Data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		select 
		cst_id,
		cst_key,
		TRIM(cst_firstname) cst_firstname,
		TRIM(cst_lastname) cst_lastname,
		case when upper(TRIM(cst_marital_status)) = 'S' then 'Single'
			when upper(TRIM(cst_marital_status)) = 'M' then 'Married'
			else 'n/a'
		end as cst_marital_status,
		case when upper(TRIM(cst_gndr)) = 'F' then 'Female'
			when upper(TRIM(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
		end as cst_gndr,
		cst_create_date
		from (
			select 
			*,
			ROW_NUMBER()over(partition by cst_id order by cst_create_date desc) as flag_last 
			from bronze.crm_cust_info
			where cst_id is not null
		)t where flag_last = 1 ;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE()
		Print '>> Truncating table silver.crm_prd_info';
		Truncate table silver.crm_prd_info;
		Print '>> Inserting Data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		case UPPER(TRIM(prd_line)) 
			when 'M' THEN 'Mountain'
			when 'R' THEN 'Road'
			when 'S' THEN 'Other Sales'
			when 'T' THEN 'Touring'
			ELSE 'n/a'
		end as prd_line,
		CAST(prd_start_dt as DATE) as prd_start_dt,
		CAST(LEAD(prd_start_dt)over(partition by prd_key order by prd_start_dt)-1 AS DATE) as prd_end_dt
		from bronze.crm_prd_info;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading crm_sales_Details
		SET @start_time = GETDATE();
		Print '>> Truncating table silver.crm_sales_details';
		Truncate table silver.crm_sales_details
		Print '>> Inserting Data into: silver.crm_sales_details';
		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		Select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt as VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt as VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt as VARCHAR) AS DATE)
		END AS sls_due_dt,

		CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
			  then sls_quantity * abs(sls_price)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0
			  then sls_sales / nullif(sls_quantity,0)
			else sls_price
		end as sls_price
		from bronze.crm_sales_details;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		-- Loading erp_cust_az12
		SET @start_time = GETDATE();
		Print '>> Truncating table silver.erp_cust_az12';
		Truncate table silver.erp_cust_az12
		Print '>> Inserting Data into: silver.erp_cust_az12';
		insert into silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		select 
		case when cid like 'NAS%' then substring(cid,4,len(cid))
		else cid
		end as cid,
		case when bdate > getdate() then null
			 else bdate
		end as bdate,
		case when Upper(TRIM(gen)) IN  ('F','FEMALE') Then 'Female'
			 when Upper(TRIM(gen)) IN  ('M','MALE') Then 'Male'
			 else 'n/a'
		end as gen
		from bronze.erp_cust_az12;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		-- Loading erp_loc_a101
		 SET @start_time = GETDATE();
		Print '>> Truncating table silver.erp_loc_a101';
		Truncate table silver.erp_loc_a101
		Print '>> Inserting Data into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101(
		cid,
		cntry
		)
		select 
		REPLACE(cid,'-','') as cid,
		case 
			when trim(cntry) = 'DE' Then 'Germany'
			when trim(cntry) IN ('US','USA') Then 'United States'
			when trim(cntry) = '' OR cntry is NULL Then 'n/a'
			else trim(cntry)
		end as cntry
		from bronze.erp_loc_a101;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		Print '>> Truncating table silver.erp_px_cat_g1v2';
		Truncate table silver.erp_px_cat_g1v2
		Print '>> Inserting Data into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END

exec silver.load_silver
