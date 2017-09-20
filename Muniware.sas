/***************************************************************************************************************************************************************
   Author          : Chetan Adhikari
   Date Created    : 09/15/2017
   Purpose         : It is about the DI Properties.The following programs extract the AUC codes from Asset database and the merge the files with
                     the files sent from the municipalities and apply other necessary action as required. Need to edit the program based on the file
                     types as well as the required conditions. 
                     
/*******************************Pathway to Asset Data**************/	
%let username = asset_reporter;
%let password = Asset123;
%let path = ASSET_P.MA.GOV.AB.CA; 
/*Macro variables to change*/
%let AsmntYear = 2016;
%let SingleMunc = 49;/*Change TJ Number*/
%let File1 = Camrose_test_file; /*Change file1 name- without school support data*/
%let File2 = MunAffSch; /*Change File2 Name- with school support data*/
%let outfile=C:\_LOCALdata\Chetan\xml\Serenic;
filename Schdata "C:\_LOCALdata\Chetan\xml\Serenic\&file2..csv" TERMSTR=CR; /*This is for Mac output*/ 

/*end macro variables*/
options center nodate nonumber orientation=landscape missing= ' ' leftmargin=.35in rightmargin=.35in topmargin=.35in bottommargin=.35in linesize=256;   
%let connect = connect to oracle(user=&username password=&password path=&path buffsize=1000);
libname ASSET oracle user=asset_reporter password=Asset123 path=ASSET_P.MA.GOV.AB.CA schema=asset; 
/****For AUC code from Asset data****/
proc sql;
&connect;
** try to get as few duplicates as possible. This will weed out ANN/RANNs. There will be only one AUG/predominant/secondary use;
	create table Basic_&SingleMunc as  select * from connection to oracle
	(select	A.municipality_name, 
				A.municipality_code, 
				A.assessment_year,
         		A.asmnt_roll_type_code,  
				A.ap_identifier,
				A.municipal_roll_nbr,  
				total_land_asmnt,
				total_improvement_asmnt,
				total_farmland_asmnt,
				total_ap_asmnt,
				ASMNT_ROLL_DATE,
				asmnt_eff_date,
				property_size, 
				prop_size_unit_of_measure as UM,
				actual_use_group_code,
				predom_actual_use_code, 
				predom_actual_use_desc,
         		secondary_actual_use_code, 
				secondary_actual_use_desc     
	from 		asgm_assessment_roll A,
				asgm_ap_characteristics B
	where 	A.municipality_code=&SingleMunc
    and 		A.assessment_year=&AsmntYear
    and 		asmnt_roll_type_code in ( 'ANN','RANN')
	and		A.ap_identifier = B.ap_identifier);

	disconnect from oracle;
quit;
proc sort data = Basic_&SingleMunc ;
	by municipal_roll_nbr ASMNT_ROLL_DATE;
run;
data Asset_&SingleMunc;
	set Basic_&SingleMunc;
	by municipal_roll_nbr ASMNT_ROLL_DATE;
	if last.municipal_roll_nbr;
	drop asmnt_eff_date of_numeric_;
	if missing(coalesceC(of _character_)) and missing(coalesce(of_numeric_)) then delete;
run;
/***import the first serenic file-Master file */
options validvarname = v7;
data WORK.camrose_&SingleMunc    ;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile "C:\_LOCALdata\Chetan\xml\Muniware\&file1..xlsx"  MISSOVER DSD lrecl=32767 firstobs=2 ;
	   informat MunicipalityCode $4.;
       informat MunicipalName $50. ;
       informat AsmtYear 4. ;
	   informat RollNumber 20.;  
	   informat Undeclared 5.;
	   informat Public 5.;
	   informat Separate 5.;
       informat AsseName $40. ;
       informat Address1 $40. ;
       informat Address2 $40. ;
       informat Address3 $40. ;
	   informat City $40. ;
       informat Prov $5. ;
	   informat PostalCode $7. ;
       informat Country $10. ;     
	   informat Lot $10. ;
       informat BLK $10. ;
       informat Plan $10. ;
	   informat QuadPortion $10. ;
	   informat QS $5.;
	   informat SEC 5.;
       informat TWP 5.;
	   informat RGE 5.;
	   informat MER 5.;  
	   informat AreaCode $5.;
	   informat Zoning $60. ;
	   informat Suite$6.;
	   informat StreetNumber $10.;
	   informat StreetName $40.;
	   informat ParcelSize 6.2 ;
	   informat NewAsmntCode 10. ;
	   informat NewAsmntValue 15.; 
       informat Residential comma15.;
	   informat NonResidential comma15.;
	   informat TotalME comma15.;
	   informat TotalFarmland comma15.;
	   informat TotalLinear comma15.;
	   informat TotalTaxable comma15.;
	   informat TotalGIL comma15.;
	   informat TotalExempt comma5.;
	   informat LiabilityCode $5.;
	   informat PropClass 5.;	              	  
       format MunicipalityCode $4. ;
       format MunicipalName $50. ;
       format AsmtYear 4.;
	   format RollNumber 20.;
	   format Undeclared 5.;
	   format Public 5.;
	   format Separate 5.; 
       format AsseName $40. ;
	   format Address1 $40. ;
       format Address2 $40. ;
       format Address3 $40. ;
	   format City $40. ;
       format Prov $5. ;
	   format PostalCode $7. ;
       format Country $10. ;        
       format Lot $10. ;
       format BLK $10. ;
	   format Plan $10. ;
	   format QuadPortion $10. ;
	   format QS $5.;
	   format SEC 5.;
       format TWP 5.;
	   format RGE 5.;
	   format MER 5.;
	   format AreaCode $5.;
	   format Zoning $60. ;
	   format Suite$6.;
	   format StreetNumber $10.;
	   format StreetName $40.;
	   format ParcelSize 6.2 ;
	   format NewAsmntCode 10. ;
	   format NewAsmntValue 15.; 
	   format Residential comma15.;
	   format NonResidential comma15.;
	   format TotalME comma15.;
	   format TotalFarmland comma15.;
	   format TotalLinear comma15.;
	   format TotalTaxable comma15.;
	   format TotalGIL comma15.;
	   format TotalExempt comma5.;
	   format LiabilityCode $5.;
	   format PropClass 5.;
       input
              MunicipalityCode $
              MunicipalName $
              AsmtYear
			  RollNumber
			  Undeclared
			  Public
			  Separate
              AsseName $
              Address1 $
              Address2 $
              Address3 $
			  City $
              Prov $
			  PostalCode $
              Country $	 			         
			  Lot $
              BLK $
              Plan $
			  QuadPortion $10. 
	          QS 
	          SEC 
              TWP 
	          RGE 
		      MER 
              AreaCode $
		     Zoning $
		     Suite $
		     StreetNumber $
		     StreetName $
	         ParcelSize 
		     NewAsmntCode 
		     NewAsmntValue 
			 Residential 
	         NonResidential 
	         TotalME 
	         TotalFarmland 
		     TotalLinear 
		     TotalTaxable 
	         TotalGIL 
	         TotalExempt 
	         LiabilityCode $
	         PropClass   
             ;
			 _infile_=compress(_infile_,"'");
    if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
	if missing(coalesceC(of _character_)) and missing(coalesce(of_numeric_)) then delete;
    run;
		proc import datafile= "C:\_LOCALdata\Chetan\xml\Muniware\&file1..xlsx"
out=all
dbms=xlsx replace;
run;
