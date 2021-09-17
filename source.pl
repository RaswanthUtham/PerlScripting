#!/usr/bin/env perl

use warnings;
use batch_adp_common_utils;
use Getopt::Long;
use Date::Format;
use Net::SFTP;
use File::Copy;
##  ETrade utils
use XML::XPath;
use et_config;
use et_log;
use et_status;
use et_signal;
use et_email;
use et_db;
##  ET inits need to be first
Et_Log_Init();
Et_Signal_Init();
Et_Config_Init();
Et_Status_Init();

my $env = POSIX::getenv("ET_ENVIRONMENT");

my %options=();
my $result= &GetOptions(\%options,
                                         "data_file=s",
                                         "data_file1=s",
                                         "data_file_pos=s",
                                         "cfg=s",
                                         );


my $option_pairing_retail_adp_rpt_file =  Et_Config_Value("option_pairing_retail_adp_rpt_file");
my $optPair_univSprd_audit_rpt_file =  Et_Config_Value("optPair_univSprd_audit_rpt_file");
my $option_pair_rpt_file =  Et_Config_Value("option_pair_rpt_file");
my $option_ool_rpt_file =  Et_Config_Value("option_ool_rpt_file");


#add dependency to bal pdt delta job

my $data_dir = Et_Config_Value("data_dir");
my $work_dir = Et_Config_Value("work_dir");
my $BP_FILE= "$data_dir/$options{'data_file'}";
my $BAL_OL_FILE= "$data_dir/$options{'data_file1'}";
my $POS_FILE = "$data_dir/ETCBO_ETC_B212_POSITIONS_FULL.237";
my $balance_date=&getBalanceAsOfDate($BP_FILE) ;
my $option_excpt_rpt1="$work_dir/Option_Level_Exceptions" ;
my $option_excpt_rpt2="$work_dir/Option_Requirement_Differences" ;
my $equity_file=Et_Config_Value("equity_file");
my $conc_req_file=Et_Config_Value("conc_req_file");
my $ftp_url=Et_Config_Value("ftp_url");
my $ftp_userid=Et_Config_Value("ftp_userid");
my $ftp_password=Et_Config_Value("ftp_password");
my $ftp_directory=Et_Config_Value("ftp_directory");
my $tpp_eqty_file="$work_dir/tpp_equity_req_".$balance_date.".dat";
my $tpp_conc_file="$work_dir/tpp_conc_req_".$balance_date.".dat";
my $product_info_file=Et_Config_Value("prod_file");
my $recon_rpt_ftp_url;
my $recon_rpt_ftp_dir;
my $ftp_recon_dir;
my $etso_port = $ENV{ETSO_PROXY_PORT};
my $etso_host = $ENV{ETSO_PROXY_HOST};
my $environ= uc $ENV{ET_ENVIRONMENT};
my $email_list = Et_Config_Value( "OOL_REPORT_" . "$environ"); 
my $mailhost = $ENV{ET_SMTP_SERVER};

$recon_rpt_ftp_url=Et_Config_Value("recon_rpt_ftp_url");
$recon_rpt_ftp_dir=Et_Config_Value("recon_rpt_ftp_dir");

my $bo_acct_info_file = Et_Config_Value("bo_acct_info_file");
print "balance date $balance_date\n";

#Global Equity 
my $final_tpp_file="/etrade/tmp/final_tpp_file";
my $final_tpp_file_s="/etrade/tmp/final_tpp_file.s";
my $final_tpp_file_adpnum="/etrade/tmp/final_tpp_file.adpnum";
my $adp_num_exch="/etrade/tmp/adp_num_exch";
my $adp_num_exch_eq_s="/etrade/tmp/adp_num_exch.eq.s";
my $adp_num_exch_sort="/etrade/tmp/adp_num_exch.s";
my $temp_tpp_file="/etrade/tmp/temp_tpp_eqty_file.txt";
my $eqty_file_t="/etrade/tmp/temp_eqty_sorted_file_t.txt";
my $eqty_file_s="/etrade/tmp/temp_eqty_sorted_file.txt";
my $prod_file_t="/etrade/tmp/temp_prod_sorted_file_t.txt";
my $prod_file_s="/etrade/tmp/temp_prod_sorted_file.txt";

my $header="H|TPP_EQTY_FILE|\n";
my $trailer="T|TPP_EQTY_FILE";
my $desc="Symbol|global req|adj option percent|ADP Number|Exchange Code|% for Margin|Max Price|\n";

print "Global Eq -1\n";

`sort -k1 -t'|' $equity_file |awk -F\"|\" '{print \$0}' > $eqty_file_t`;
`cat $eqty_file_t |awk -F\"|\" '{print \$1 ":^" \$2}' | sort -t'^'|sed 's/\\^/|/g' > $eqty_file_s`;
print "Global Eq -2\n";
`sort -k1 -t'|' $product_info_file |awk -F\"|\" '{print \$0}' > $prod_file_t`;
`cat $prod_file_t |grep -v '|OPTN|'|awk -F\"|\" '{print \$1 ":^" \$0}'|sort -t'^'|sed 's/\\^/|/g' > $prod_file_s`;
`join -t '|' -11 -21 -o 1.1 1.2 2.4 2.5 2.6 2.18 $eqty_file_s $prod_file_s | sed 's/ //g' |sed 's/\\^/|/g' |sed 's/://g'  >$temp_tpp_file`;
print "Global Eq -3\n";
`cat $temp_tpp_file |awk -F"|" '{if ( (\$3 == "EQ") && (\$4 == 2) && !((\$5 == "NSOB") || (\$5 == "NSBB" ) )  )sub(\$2,"100.000"); print \$1"|"\$2"|"\$6 }'>$final_tpp_file`;
  unless (open($fh_report, ">$tpp_eqty_file")) {
    FatalError("Could not open output file $out_file", GC_ERROR_DB_SQL, "30", __LINE__);
    }
print "Global Eq -4\n";
`cat $POS_FILE |grep -v \"|OPTN|\" |cut -d'|' -f6,4,51 | sort -u | awk -F\"|\" '{print \$2"^"\$1"|"\$3}' |sort -t '^'  >  $adp_num_exch` ;
`cat $adp_num_exch |sed 's/\\^/:^/g' |sort -t'^' > $adp_num_exch_eq_s`;
print "Global Eq -5\n";
`cat $final_tpp_file|awk -F"|" '{print \$1":^"\$2 "|" \$3}' |sort -t'^' > $final_tpp_file_s`;
`join -t '^' $adp_num_exch_eq_s $final_tpp_file_s |awk -F"^" '{print \$1 "|" \$3 "|" \$2}'|sed 's/://g' > $final_tpp_file_adpnum`; 
print "Global Eq -6\n";

 print $fh_report $header;
 print $fh_report $desc;
my $eqtyFile=`cat $final_tpp_file_adpnum`;
print $fh_report $eqtyFile;
print $fh_report $trailer;
print "Global Eq -7\n";


#Conc Req changes
$conc_req_sort="/etrade/tmp/conc_req_sort";
$conc_req_sort_match="/etrade/tmp/conc_req_sort_match";
$conc_req_sort_mismatch="/etrade/tmp/conc_req_sort_mismatch";
$conc_req_sort_join="/etrade/tmp/conc_req_sort_join";
$final_tpp_file_conc="/etrade/tmp/conc_req_final";
$adp_num_bond="/etrade/tmp/adp_num_bond";
$adp_num_acct_symb="/etrade/tmp/adp_num_acct_symb";
my $conc_header="H|TPP_CONC_FILE|\n";
my $conc_trailer="T|TPP_CONC_FILE";
my $conc_desc="Account_Id|Symbol|Long_Conc_Req|Short_Conc_Req|Short_Option_Conc_Req|ADP Number|Exchange Code\n";


print "Conc Req -1\n";
unless (open($conc_fh_report, ">$tpp_conc_file")) {
    FatalError("Could not open output file $tpp_conc_file", GC_ERROR_DB_SQL, "30", __LINE__);
    }
 print $conc_fh_report $conc_header;
 print $conc_fh_report $conc_desc;

#`sort -k2 $conc_req_file >$conc_req_sort`;
`cat $conc_req_file |awk -F"|" '{print \$1 "|" \$2 "^" \$0}' |sort -t'^' >$conc_req_sort`;
print "Conc Req -2\n";
#`cat $adp_num_exch |sed 's/\\^/|/g' > $adp_num_exch_sort`;
`cat $POS_FILE |grep -v \"|OPTN|\" | awk -F"|" '{print \$1 "|" \$6 "^"  \$4 "|" \$51}' | sort -t'^' > $adp_num_acct_symb`;
print "Conc Req -3\n";
`cat $POS_FILE |grep "|BOND|" |awk -F"|" '{print \$6 "^"}' |sort -t'^' > $adp_num_bond`;
print "Conc Req -4\n";
#`join -a1 -1 2 -2 1 -t "|" -o 1.1 1.2 1.3 1.4 1.5 2.2 2.3 -e "" $conc_req_sort $adp_num_exch_sort > $final_tpp_file_conc`;
`join  -a1 -t'^' -e "||" $conc_req_sort  $adp_num_acct_symb |cut -d'^' -f2,3 |grep "\\^" | sed 's/\\^/|/g'  > $conc_req_sort_match`;
print "Conc Req -5\n";
`join  -a1 -t'^' -e "||" $conc_req_sort  $adp_num_acct_symb  |cut -d'^' -f2,3 |grep -v "\\^" |sed 's/\$/||/g'  > $conc_req_sort_mismatch`;
print "Conc Req -6\n";
`cat $conc_req_sort_match $conc_req_sort_mismatch|uniq > $conc_req_sort_join`;

#filter matching bonds
`cat $conc_req_sort_join|fgrep -v -f $adp_num_bond |cut -d'^' -f2  > $final_tpp_file_conc`;
my $concFile=`cat $final_tpp_file_conc`;
print $conc_fh_report $concFile;
print $conc_fh_report $conc_trailer;

#`rm /etrade/tmp/temp_eqty_sorted_file.txt /etrade/tmp/temp_prod_sorted_file.txt`;

# Changes to generate bond_req_info file.
my $temp_bond_file;
my $temp_dir="/etrade/tmp";
my $conc_req_mod;
my $bond_req_info_mod;
my $bond_req_info_mod_tmp;
my $bond_req_info_mod_tmp_s;
my $conc_req_mod_tmp;
my $conc_req_mod_tmp_s="$temp_dir/conc_req_mod.tmp.s";
my $bond_req_info_final1="$temp_dir/bond_req_info.final1";
my $adp_num="$temp_dir/adp_num";
my $adp_num_s="$temp_dir/adp_num.s";
my $adp_num_s1="$temp_dir/adp_num.s1";
my $bond_req_info_final1_symb="$temp_dir/bond_req_info.final1.symb";
my $bond_req_info_final1_cusip="$temp_dir/bond_req_info.final1.cusip";
my $mar_bal="$temp_dir/mar_bal";
my $mar_bal_s="$temp_dir/mar_bal.s";
my $bond_req_info_final1_cusip_mbal_ac="$temp_dir/bond_req_info.final1.cusip.mbal.ac";
my $bond_req_info="$work_dir/tpp_bond_req_".$balance_date.".dat";
$temp_bond_file="$work_dir/temp_bond_req_info.txt";
$conc_req_mod="$temp_dir/conc_req_mod";
$bond_req_info_mod="$temp_dir/bond_req_info.mod";
$bond_req_info_mod_tmp="$temp_dir/bond_req_info.mod.tmp";
$bond_req_info_mod_tmp_s="$temp_dir/bond_req_info.mod.tmp.s";
$conc_req_mod_tmp="$temp_dir/conc_req_mod.tmp";
$bond_req_info_final1_tmp="$temp_dir/bond_req_info_final1.tmp";
my $global_bond_req_file="$temp_dir/global_bond_req_file";
my $global_bond_req_file_s="$temp_dir/global_bond_req_file.s";
my $bond_req_info_s="$temp_dir/bond_req_info.s";
my $bond_req_info_final_temp="$temp_dir/bond_req_info_final._temp";
my $tmp_bnd_file1="$temp_dir/tmp_bnd_file1";
my $tmp_bnd_file2="$temp_dir/tmp_bnd_file2";
my $prev_biz_date_mmddyyyy;
my $prev_biz_date_yyyymmdd;
my $second_prev_biz_date_mmddyyyy;
my $second_prev_biz_date_yyyymmdd;

my $current_date = `date +%Y%m%d`;
chomp($current_date);
# Get Prev Biz Date
my $zipped_temp_bond_req_file=$temp_bond_file.".".$current_date.".gz";
print "Zipped filename:$zipped_temp_bond_req_file";

########## trim() ##########
sub trim($)
{
  my $string = shift;
  if (not defined $string) {return "";}
  $string =~ s/^\s*//;
  $string =~ s/\s*$//;
  return $string;
}

sub wsGetSymbolNo($) {
	my ($symbName) = @_;
	my $symb_no;
	Et_Run_Log("Inside wsGetSymbolNo...");
	my $wsURLForWFCaseSvc="http://${etso_host}.etrade.com:${etso_port}/xml_request.xml";
	#my $wsURLForWFCaseSvc="http://sit161w80m7.etrade.com:8008/xml_request.xml";
	my $xmlRqst = "<Product_QueryProducts>
 <Count>1</Count>
<!-- This is a list -->
 <ProductIds >
  <Symbol>$symbName</Symbol>
       <ExchangeCode>US</ExchangeCode>
        <TypeCode>OPTN</TypeCode>
</ProductIds>
<Originator>4</Originator>
</Product_QueryProducts>";
	my $xmlResp =
		&httppost($xmlRqst, $wsURLForWFCaseSvc, "Product_QueryProducts" );

	if ( $xmlResp eq $BLANK ) {
		Et_Run_Log("Product_QueryProducts-BLANKRESP\n");
	}
	else {
		Et_Run_Log("Got response from Service Product_QueryProducts");
		my $xp = XML::XPath->new( xml => $xmlResp );
		$symb_desc = $xp->findvalue('/Product_QueryProductsResponse/ProdComm/SymbolDesc');
		#$symb_no = $xp->findvalue('//Product_QueryProductsResponse/ProdAddl/Equity/AliasList/Alias[AliasType= 11]/AliasCode');
	}

	if ( $symb_desc ne '' ) {
		Et_Run_Log("Institution Number of Account $acctNumber is $symb_no");
	}

	return $symb_desc;
}

#
# Submit the specified service request to the specified server, return the service response XML for success or throw an exception if failed.
#
sub httppost {
	my ( $xmlRqst, $wsURL, $svcName ) = @_;

	Et_Run_Log("Server URL: $wsURL");
	Et_Run_Log("Request XML:\n$xmlRqst\n");

	my $ua = LWP::UserAgent->new;
	$ua->timeout( [240] );

	my $req = HTTP::Request->new( POST => $wsURL );
	$req->content_type('application/x-www-form-urlencoded');

	my $i         = 0;
	my $errorCode = 1;
	my $errorDescription;
	my $xmlResp = "";

	my $maxHttpTries = 2;

	for (
		$i = 0 ;
		( $i < $maxHttpTries )
		  && ( $errorCode != 0 && $errorCode != $DATA_NOT_FOUND ) ;
		$i++
	  )
	{
		$req->content("");    # initialize it to avoid any stale data
		$req->content("$xmlRqst");

		my $res = $ua->request($req);
		$xmlResp = "";

		if ( !( $res->is_success ) ) {
			Et_Error_Log( "Failed to invoke $wsURL");
		}
		else {
			$xmlResp = $res->content;
		}

		Et_Run_Log("Response XML:\n$xmlResp\n");

		# Check for errors in the output
		$errorCode        = 0;
		$errorDescription = "";

		if ( $xmlResp =~ /<errorcode>\D?(\d+)<\/errorcode>/ ) {
			$errorCode = $1;

			if ( $errorCode eq $DATA_NOT_FOUND ) {
				$errorDescription = "Data not found";

				logInfo("The service $svcName returned Data not found");

				last;    # No need to retry.
			}
			else {
				if ( $xmlResp =~ /<message>(.*)<\/message>/ ) {
					$errorDescription = $1;
				}
				else {
					$errorDescription = "Unknown error";
				}

				Et_Error_Log(
					"The $i-th call to the service $svcName failed"
				);
			}
		}
	}

	if ( $errorCode != 0 ) {
		die "$errorCode: $errorDescription";
	}

	return ($xmlResp);
}

($prev_biz_date_mmddyyyy,$prev_biz_date_yyyymmdd)=&getprevBizDateWpr($current_date);
($second_prev_biz_date_mmddyyyy, $second_prev_biz_date_yyyymmdd)= &getprevBizDateWpr($prev_biz_date_yyyymmdd);
($third_prev_biz_date_mmddyyyy, $third_prev_biz_date_yyyymmdd)= &getprevBizDateWpr($second_prev_biz_date_yyyymmdd);

if(! -e "$zipped_temp_bond_req_file")
{
 Et_Run_Log("Todays zip File [ $zipped_temp_bond_req_file ] does not exist. Looking for prev day's file");
 $zipped_temp_bond_req_file=$temp_bond_file.".".$prev_biz_date_yyyymmdd.".gz";
 if(! -e "$zipped_temp_bond_req_file")
  {
    Et_Run_Log("Prev Biz days zip File [ $zipped_temp_bond_req_file ] does not exist. Looking for sec prev day's file"); 
    $zipped_temp_bond_req_file=$temp_bond_file.".".$second_prev_biz_date_yyyymmdd.".gz";
  }

}
print"Latesdt zip file picked : $zipped_temp_bond_req_file ";

`gunzip -c $zipped_temp_bond_req_file > $tmp_bnd_file1`;
`mv $temp_bond_file $tmp_bnd_file2`;
`cat $tmp_bnd_file1 $tmp_bnd_file2>$temp_bond_file`;
&generatebondReqFile();
&generateEquityReqFile();
`cat $conc_req_file|awk -F\"|\" '{if ( (\$3>0) || (\$4 > 0)) print \$0}' >$conc_req_mod`;
`cat $temp_bond_file|awk -F"|" '{print \$1"|"\$2"|"\$3"|"\$4"|"\$5"|"\$6"|"\$7"|"\$8"|"\$9"|"\$10"|"\$11"|"\$12"|"\$13"|"\$14"|"\$15*100}' > $bond_req_info_mod`; 
`cat $bond_req_info_mod |awk -F\"|\" '{print \$1"|"\$2"^"\$0}' > $bond_req_info_mod_tmp`;
`sort -t'^' $bond_req_info_mod_tmp > $bond_req_info_mod_tmp_s`;
`cat $conc_req_mod |awk -F"|" '{print \$1"|"\$2"^"\$3}' > $conc_req_mod_tmp`;
`sort -t'^' $conc_req_mod_tmp >  $conc_req_mod_tmp_s`;
`join -a1 -1 1 -2 1 -t"^" -o 1.2 2.2 -e "No Conc Req" $bond_req_info_mod_tmp_s $conc_req_mod_tmp_s > $bond_req_info_final1_tmp`;
`sed 's/\\^/|/g' $bond_req_info_final1_tmp > $bond_req_info_final1`;
`cut -d'|' -f4,6 $POS_FILE > $adp_num`;
`cat $adp_num|awk -F"|" '{print \$2"^"\$1}' |sort -t '^' > $adp_num_s`;
`sort -u $adp_num_s > $adp_num_s1`;
`cat $bond_req_info_final1 |awk -F"|" '{print \$2 "^" \$0}' |sort -t'^' > $bond_req_info_final1_symb`;
`join -t '^' $adp_num_s1 $bond_req_info_final1_symb| awk -F"^" '{print \$3 "|" \$2}'  > $bond_req_info_final1_cusip`;
`cat $BP_FILE| awk -F"|" '{print \$1 "^" \$38}' > $mar_bal`;
`cat $mar_bal|sort -t'^' > $mar_bal_s`;
`cat $bond_req_info_final1_cusip|awk -F"|" '{print \$1 "^" \$0}' |sort -t'^' > $bond_req_info_final1_cusip_mbal_ac`;
`join -t'^' $mar_bal_s $bond_req_info_final1_cusip_mbal_ac |awk -F"^" '{print \$3 "|" \$2}'  > $bond_req_info_final_temp`;
`sort -k2 -t'|' $bond_req_info_final_temp > $bond_req_info_s`;
`sort -k1 -t'|' $global_bond_req_file > $global_bond_req_file_s`;

`join -a1 -1 2 -2 1 -t"|"  -e "0" -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 2.2 $bond_req_info_s $global_bond_req_file_s > $bond_req_info`;
#`cd /etrade/tmp`;
#`rm bond_req_info.mod bond_req_info.mod.tmp bond_req_info.mod.tmp.s adp_num adp_num.s adp_num.s1 conc_req_mod.tmp conc_req_mod.tmp.s bond_req_info.final1 bond_req_info.final1.symb bond_req_info.final1.cusip bond_req_info.final1.cusip.mbal.ac`;
print "Recon FTPDIR: $recon_rpt_ftp_dir";
my $status=&sendReconFileToFTP($recon_rpt_ftp_url,$ftp_userid,$ftp_password,$recon_rpt_ftp_dir,$tpp_eqty_file);
print"status of FTP of file  $tpp_eqty_file:$status ";

$status=&sendReconFileToFTP($recon_rpt_ftp_url,$ftp_userid,$ftp_password,$recon_rpt_ftp_dir,$tpp_conc_file);
print"status of FTP of file  $conc_req_file:$status ";

$status=&sendReconFileToFTP($recon_rpt_ftp_url,$ftp_userid,$ftp_password,$recon_rpt_ftp_dir,$bond_req_info);
print"status of FTP of file  $bond_req_info:$status ";

print"FTPDIR:$ftp_directory ";

$option_pairing_retail_adp_rpt_file =  $option_pairing_retail_adp_rpt_file.'_'.$balance_date.'.'.'dat';
$optPair_univSprd_audit_rpt_file =  $optPair_univSprd_audit_rpt_file.'_'.$balance_date. '.'.'dat';
$option_excpt_rpt1= $option_excpt_rpt1.'_'.$balance_date.'.'.'dat'; 
$option_excpt_rpt2= $option_excpt_rpt2.'_'.$balance_date.'.'.'dat'; 


my $option_pairing_retail_adp_rpt_file_tmp = "$option_pairing_retail_adp_rpt_file" . ".tmp";
my $res = `cp $option_pairing_retail_adp_rpt_file $option_pairing_retail_adp_rpt_file_tmp`;
 print"Option ph1 rpt-  $option_pairing_retail_adp_rpt_file \n";
 print"Option ph1 rpt -tmp-  $option_pairing_retail_adp_rpt_file_tmp \n";
 print"Option ph2 rpt -  $optPair_univSprd_audit_rpt_file \n";

my $cmd= "cut -d'|' -f2,8,12 $optPair_univSprd_audit_rpt_file |grep '|Yes|' |grep -v '|\$-' |cut -d'|' -f1,3 |sed -e 's/\\\$//g'  > /tmp/opt_ph2.tmp";
`$cmd`;
print "Running cmd:$cmd\n";

 
`awk -F'|' '{a[\$1]+=\$2;}END{for(i in a)print i "|" a[i];}' /tmp/opt_ph2.tmp >  /tmp/opt_ph2` ;
if ( $? != 0 )
{
  print "awk operation failed\n"; 
}
  print "Af awk operation \n"; 

`sort -t'|' -k1 /tmp/opt_ph2 |sed -e 's/\$/|Yes/g' > /tmp/opt_ph2.s`;
`sort -t"|" -k2 $option_pairing_retail_adp_rpt_file |egrep -v "H|Run Date" > /tmp/opt_ph1.s`;

#matching lines from op ph1 & op ph2 -(For Univ Spread flag -YES)
`join -t'|' -12 -21 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 1.19 1.20 1.21 1.22 1.23 1.24 1.25 1.26 1.27 1.28 1.29  2.3 2.2 /tmp/opt_ph1.s /tmp/opt_ph2.s > /tmp/opt_ph1.final.1`;

#non matching lines from op ph1 & op ph2 -(For Univ spread flag -NO)
`join -t'|' -1 2 -v1 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 1.19 1.20 1.21 1.22 1.23 1.24 1.25 1.26 1.27 1.28 1.29 /tmp/opt_ph1.s /tmp/opt_ph2.s |sed -e 's/\$/|No|/g' > /tmp/opt_ph1.final.2`;

`cat /tmp/opt_ph1.final.1 /tmp/opt_ph1.final.2 |sort -t'|' -k1 > /tmp/opt_ph1.final.tmp`;

`cut -d'|' -f1,8 $BAL_OL_FILE|grep -v "|0" |egrep -v "H|T" |cut -d'|' -f1|sed -e 's/\$/|Y/g' |sort -t"|" -k1 > /tmp/ol.s`;

`join -t'|' -12 -21 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 1.19 1.20 1.21 1.22 1.23 1.24 1.25 1.26 1.27 1.28 1.29  1.30 1.31 2.2 /tmp/opt_ph1.final.tmp /tmp/ol.s > /tmp/opt_ph1.final.tmp.1`;

`join -t'|' -12 -21 -v1 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 1.19 1.20 1.21 1.22 1.23 1.24 1.25 1.26 1.27 1.28 1.29  1.30 1.31 /tmp/opt_ph1.final.tmp /tmp/ol.s |sed -e 's/\$/|N/g' > /tmp/opt_ph1.final.tmp.2`;

`head -1 $option_pairing_retail_adp_rpt_file > /tmp/opt_ph1.final`;
`echo "Run Date|Account#|Lvl|MRGN|CA?|ShtOpt?|Stock?|T++HseMaintReq|BPS HseMaintReq|Diff HseMaintReq|%EQ HseMaintDiff|T++ OptMaintReq|BPS OptMaintReq|Diff OptMaintReq|%EQ OptMaintDiff|T++ LiqEQ|BPS LiqEQ|Diff LiqEQ|T++ MrgnEQ|BPS MrgnEQ|Diff MrgnEQ|BPS Type1 SweepBal|BPS Type1 CashBal|T++ HseEX|BPS HseEX|Diff HseEX|T++ ExchEX|BPS ExchEX|Diff ExchEX|Max Risk Used|Diff between Req|Type7CashIndicator" > /tmp/opt_ph1.final`;
`cat /tmp/opt_ph1.final.tmp.1 /tmp/opt_ph1.final.tmp.2 >> /tmp/opt_ph1.final`;
`cp /tmp/opt_ph1.final $option_pairing_retail_adp_rpt_file`;

#Exception rpt1
`echo "Run Date | Account# | Lvl | MRGN " > $option_excpt_rpt1`;
`cat /tmp/opt_ph1.final|awk -F"|" '{if ((\$3 == 0) || ( (\$3 == 4) && ( (\$4  == "I") || (\$4 == "C") ) ) || ( (\$3 == 3) && (\$4 == "C")) )print \$1 "|" \$2 "|" \$3"|"\$4}'  >> $option_excpt_rpt1`;

#Exception rpt2
`echo "Run Date | Account# |Notes | Lvl | MRGN | T++ OptMaintReq | BPS OptMaintReq | Diff OptMaintReq | %EQ OptMaintDiff | Universal Used | Universal Improvement | Diff HseMaintReq | T++ HseEX | BPS HseEX | Diff HseEX | T++ ExchEX | BPS ExchEX | Diff ExchEX" > $option_excpt_rpt2`;
`cat /tmp/opt_ph1.final|sed -e 's/,//g' |awk -F"|" '{if ( ( (\$3 > 0) || ((\$3 == 0) && (\$4 == "M")) ) &&  ((\$10 > 1.99) || (\$10 < -1.99) ) && ( (\$15 > 9.99) || (\$15 <-9.99)) && ( (\$30 == "No") || ((\$30 == "Yes") && ((\$31 > 1000) || (\$31 < 0 && \$31< -1000))) ) )  print \$1 "|" \$2"||" \$3 "|" \$4 "|" \$12 "|" \$13 "|" \$14 "|" \$15 "|" \$30 "|" \$31 "|" \$10 "|" \$24 "|" \$25"|" \$26"|" \$27"|" \$28"|" \$29}' |sort -n -r -k9 -t'|' >> $option_excpt_rpt2`;

#Outoflevel report
$option_ool_rpt_file =  $option_ool_rpt_file . "_" .$balance_date.".dat";
 print"OOL rpt -  $option_ool_rpt_file\n";
#PBS-182,Add Security Name & Age to OOL Report

my $now =`date +"%m-%d-%Y_%H-%M-%S"`;
chomp $now;
my $temp_filename = "/etrade/tmp/optn_lvl0_tempfile";
my $temp_filename1 = "/etrade/tmp/optn_lvl0_tempfile1";
my $temp_filename2 = "/etrade/tmp/optn_lvl0_tempfile2";
my $temp_ref_filename = "/etrade/tmp/optn_lvl0_ref_tempfile";
my $int_ref_filename = "/etrade/tmp/optn_lvl0_ref_intfile";
my $inter_ref_file = "/etrade/tmp/optn_lvl0";
my $temp_in_file = "/etrade/tmp/temp_in_file";
my $temp_filename3 ="/etrade/tmp/optn_lvl0_tempfile3";
my $temp_filename_dup ="/etrade/tmp/temp_filenamedup";

if( -e $temp_ref_filename){
`rm -f $temp_ref_filename`;
}
if( -e $temp_filename1){
`rm -f $temp_filename1)`;
}

`echo "H|OPTION OUT OF LEVEL ACCTS|" > $option_ool_rpt_file`;
`cat $option_pair_rpt_file |egrep "OOL Positions Info|OptionOutOfLevelFlag:1" |cut -d'|' -f1 |perl -0777pe 's/\nOOL/OOL/g' >> $temp_filename`;
if( -e $temp_filename1){
`rm -f $temp_filename1`;
}
`cut -d'|' -f1,5 $bo_acct_info_file |grep  '|1' |grep '|0' |cut -d'|' -f1 > $inter_ref_file`;
`cat $POS_FILE |grep '|OPTN|' |cut -d'|' -f1,2,6 |grep '|1|' |grep -v '|OPTN' |cut -d'|' -f1,3 |fgrep -f $inter_ref_file |sort -u >> $temp_filename`;
open(my $fh1, '<:encoding(UTF-8)', $temp_filename)
  or die "Could not open file '$temp_filename' $!";
while (my $row = <$fh1>) {
  if (index($row,'OOL') == -1)
  {
   $acct_number_temp = `echo '$row' |cut -d"|" -f1`;
   $acct_number = trim($acct_number_temp);
   chomp($acct_number);
   $position_symbol_temp = `echo '$row' |cut -d"|" -f2`;
   my $position_symbol=trim($position_symbol_temp);
   chomp($position_symbol);
   my $symbol_description=wsGetSymbolNo($position_symbol);
   if (defined $symbol_description and $symbol_description ne "")
   {
     my @symb_des = split('\$', $symbol_description);
     $symbol_description = $symb_des[0]."\\\$".$symb_des[1];
   }
   `echo "$acct_number|$symbol_description"  >> $temp_filename1`;
   next;
  }  
  $acct_number = `echo '$row' |cut -d":" -f2`;
  chomp($acct_number);
  $acct_number_temp=`echo '$acct_number'| rev |cut -c20- | rev`;
  $acct_number=trim($acct_number_temp);
  my @position_count = $row =~ /(OOL)/g;
  my $count=@position_count;
  my $add=0;
  for (my $i=0; $i < $count; $i++) {
  my $position=$i+$add+4;
  my $position_symbol_temp=`echo '$row' | cut -d":" -f$position | cut -c1-21`;
  my $position_symbol=trim($position_symbol_temp);
  my $symbol_description=wsGetSymbolNo($position_symbol);
  if (defined $symbol_description and $symbol_description ne "")
  {
    my @symb_des = split('\$', $symbol_description);
    $symbol_description = $symb_des[0]."\\\$".$symb_des[1];
  }
  $add=$add+1;
  `echo "$acct_number|$symbol_description"  >> $temp_filename1`;
  }
}
`mv $temp_filename1 $temp_filename`;

`sort -u $temp_filename >$temp_filename_dup`;
`cat $temp_filename_dup >$temp_filename`;
`rm -rf $temp_filename_dup`;

#Included logic for handling Previous option_ool_rpt_file data for age and timestamp.

my $pre_option_ool_rpt_file = "$work_dir/option_ool_rpt_file_*";
my $temp_prev_option_ool_rpt_file = `ls -1 -t $pre_option_ool_rpt_file|head -2|tail -1`;
  
  open(my $fh, '<:encoding(UTF-8)', $temp_filename)
     or die "Could not open file '$temp_filename' $!";
  `rm -rf $temp_in_file`;
  while (my $row = <$fh>) {
  Et_Run_Log("Inside OOl_Report_Creation Records ...");
  chomp($row);
  trim($row);
  my ($acct_no,$symbol) = split ('\|', $row);
  my $symbol_desc;
  if (defined $symbol and $symbol ne "")
  {
    my @symb_des = split('\$', $symbol);
    $symbol_desc = $symb_des[0]."\\\$".$symb_des[1];
  }
  chomp($acct_no);

  chomp $temp_prev_option_ool_rpt_file;
  
  my $ret1 = "cat $temp_prev_option_ool_rpt_file |grep \"$symbol_desc\" |grep \"$acct_no\" |wc -l"; 
  $ret = `$ret1`;
  
  if ($ret > 0)
  {
    `cat $temp_prev_option_ool_rpt_file |grep \"$symbol_desc\" |grep \"$acct_no\" > $temp_in_file`;
    &in_file($temp_in_file);
  }
  else
  {
  $ool_days = 1;
  $ool_timestamp1=`date +"%m-%d-%Y"`;
  $ool_timestamp1=trim($ool_timestamp1);
  `echo "$acct_no|$symbol_desc|$ool_days|$ool_timestamp1" >> $temp_filename2`;
  }
}
close $fh;
`rm -rf $temp_filename`;
my $temp_filename4 ="/etrade/tmp/temp_file";
chomp($temp_filename3);
`sort -u $temp_filename3 >$temp_filename4`;
`rm -rf $temp_filename3`;
chomp($temp_filename4);
`cat $temp_filename4 >>$temp_filename2`;
`rm -rf $temp_filename4`;
`cp $temp_filename2 $inter_ref_file`;
`echo "H|OPTION OUT OF LEVEL ACCTS|" > $option_ool_rpt_file`;
`cat $inter_ref_file >> $option_ool_rpt_file`;
`echo "T|OPTION OUT OF LEVEL ACCTS|" >> $option_ool_rpt_file`;
`rm -rf $temp_filename2 $inter_ref_file $temp_filename4 $temp_in_file`;

sub in_file($)
{
Et_Run_Log("OOL data of previous file and count Age and Timestamp...");
my $temp_in_file =shift;
my $ool_account;
my $ool_symbol_desc;
my $ool_timestamp1;
my $ool_days;
my $ool_symbol_desc1;
 open(my $fh1, '<:encoding(UTF-8)', $temp_in_file)
    or die "Could not open file '$temp_in_file' $!";
 while(my $row1 = <$fh1>)
    {
        chomp($row1);
        ($ool_account,$ool_symbol_desc,$ool_days,$ool_timestamp1) = split ('\|', $row1);
        if (defined $ool_symbol_desc and $ool_symbol_desc ne "")
        {
          my @symb_des1 = split('\$', $ool_symbol_desc);
          $ool_symbol_desc1 = $symb_des1[0]."\\\$".$symb_des1[1];
        }
        if($ool_days < 60)
        {
                $ool_days += 1;
        }
        $ool_timestamp1 = trim($ool_timestamp1);
        `echo "$ool_account|$ool_symbol_desc1|$ool_days|$ool_timestamp1" >> $temp_filename3`;
    }
        close $fh1;
}
#PBS-182,Add Security Name & Age to OOL Report
$status=&sendReconFileToFTP($recon_rpt_ftp_url,$ftp_userid,$ftp_password,$recon_rpt_ftp_dir,$option_ool_rpt_file);
print"status of FTP of file  $option_ool_rpt_file:$status ";

$status = &sendFileToFTP();
    print"status of FTP of file  $option_pairing_retail_adp_rpt_file :$status ";

#Send or update mails - PBS-2593
&process_email_report( $email_list,$option_ool_rpt_file, $mailhost  );

sub process_email_report($$$)
{
   my $email_list=shift;
   my $content = shift;
   my $mhost = shift;
   my $subject= "[$environ]" . " OOL Report";

#   open up for reading  the option_ool_rpt_file_yyyyddmm.txt
#   open up for writing /tmp/option_ool_email_rpt_timestamp.txt"
#   while reading option_ool_rpt_file_yyyyddmm.txt
#     format and write to /tmp/option_ool_email_rpt_timestamp.txt
#   end while
#  formated_content - formatted this file $option_ool_rpt_file
  Et_Run_Log("sendMailFile $subject, $email_list, $content,$mhost ");
  &sendMailFile( $subject, $email_list, $content,$mhost );

}


sub sendMailFile()
{
    ( my $mSubject ,my $mList ,my $mFile,  my $mhost) = @_ ;
    my $noOfParams =@_ ;
    if ( $noOfParams != 4 )
    {
        Et_Error_Log("Wrong number of params passed to sendMailFile. \n @_ . Usage : sendMailFile(subject,mailList,inputFile) ") ;
        return;
    }
    if ( ! -e "$mFile" )
    {
        Et_Error_Log("File name $mFile passed to sendMailFile does not exist . Usage : sendMailFile(subject,mailList,inputFile) " ) ;
        return ;
    }
    my $x=`cat $mFile` ;
  Et_Run_Log("Et_Mail: $mList,$mSubject,$x,$mhost");
    et_email::Et_Mail($mList,$mSubject,$x,$mhost) ; }


#add dependency to account_ets_parse_ol_bal_file (for BAL OL file)
sub getBalanceAsOfDate($)
{
  my $file=shift ;
  my $head_command=`which head` ;
  chomp($head_command) ;

  my $command = $head_command." -1 ".$file ;
  my $header_balance=`$command` ;

  my @date = split(/\|/,$header_balance) ;
  return $date[1] ;
}

sub sendFileToFTP {
    Et_Run_Log("Starting: sendFileToFTP()");
        my $ftpMgr = Net::FTP->new($ftp_url);

        if ( !$ftpMgr ) {
                Et_Error_Log("SendFilesToFTP() - Ftp failed for site:  $ftp_url \n");
                Et_Error_Log("SendFilesToFTP() - ftpMgr: $ftpMgr \n");
                recordErrors( "ftp failed for machine $ftp_url - result: $ftpMgr","", "" );
                return FTP_ERROR;
        }
        if ( !$ftpMgr->login( $ftp_userid, $ftp_password ) ) {
                Et_Error_Log("SendFilesToFTP() - FTP login failed for : $ftp_userid with password $ftp_password on Machine $ftp_url \n");
                recordErrors("ftp login failed for site $ftp_userid@$ftp_url - result: $ftpMgr","", "");
                return FTP_ERROR;
        }
        if ( !$ftpMgr->cwd($ftp_directory) ) {
                Et_Error_Log("SendFilesToFTP() - FTP Could not change to Directory $ftp_directory on $ftp_url\n");
                recordErrors("ftp change directory failed for $ftp_directory - result: $ftpMgr","", "" );
                return FTP_ERROR;
        }

        $ftpMgr->put($option_pairing_retail_adp_rpt_file);
        $ftpMgr->put($option_excpt_rpt1);
        $ftpMgr->put($option_excpt_rpt2);
        $ftpMgr->quit();

        Et_Run_Log("Finished: sendFileToFTP()");

        return SUCCESS;
}
sub sendReconFileToFTP($$$$$) {
    Et_Run_Log("Starting: sendReconFileToFTP():$recon_rpt_ftp_dir");
       $ftp_url=shift;
       $ftp_userid=shift;
       $ftp_password=shift;
       $ftp_recon_dir=shift;
       my $ftp_file=shift;
print "$ftp_url  $ftp_userid  $ftp_password  $ftp_directory  $ftp_file";
        my $ftpMgr = Net::FTP->new($ftp_url);

        if ( !$ftpMgr ) {
                Et_Error_Log("SendFilesToFTP() - Ftp failed for site:  $ftp_url \n");
                Et_Error_Log("SendFilesToFTP() - ftpMgr: $ftpMgr \n");
                recordErrors( "ftp failed for machine $ftp_url - result: $ftpMgr","", "" );
                return FTP_ERROR;
        }
        if ( !$ftpMgr->login( $ftp_userid, $ftp_password ) ) {
                Et_Error_Log("SendFilesToFTP() - FTP login failed for : $ftp_userid with password $ftp_password on Machine $ftp_url \n");
                recordErrors("ftp login failed for site $ftp_userid@$ftp_url - result: $ftpMgr","", "");
                return FTP_ERROR;
        }
        if ( !$ftpMgr->cwd($ftp_recon_dir) ) {
                Et_Error_Log("SendFilesToFTP() - FTP Could not change to Directory $ftp_recon_dir on $ftp_url\n");
                recordErrors("ftp change directory failed for $ftp_recon_dir - result: $ftpMgr","", "" );
                return FTP_ERROR;
        }

        $ftpMgr->put($ftp_file);
        $ftpMgr->quit();

        Et_Run_Log("Finished: sendFileToFTP()");

        return SUCCESS;
}


# ----------------------------------------------
# Make database connections --- ProductDB
# ----------------------------------------------
sub Init()
{
  my $dbserver;
  my $db_name;
  my $username;
  my $password;
  my $logdbname="UsProductDB";

  ($dbserver, $db_name, $username, $password) = et_db::get_db_info($logdbname);

  unless ( $dbserver  && $db_name && $username && $password ) {
    Et_Error_Log("db_init_connection: could not get ProductDB information") ;
    Et_Update_Status(0, "NOTOK") ;
    return GC_ERROR_DB_SQL;
  }

  $proddb_dbh = et_db::connect_to_sybase($dbserver, $db_name, $username, $password);
  unless (defined $proddb_dbh) {
     Et_Error_Log("db_init_connection: could not connect to ProductDB.") ;
     Et_Update_Status(0, "NOTOK") ;
     return GC_ERROR_DB_SQL;
     }

  my $isolation_0 = "set transaction isolation level 0";
  unless ($proddb_dbh->do($isolation_0)) {
      Et_Error_Log("db_init_connection: set isolation level 0 failed.");
      return GC_ERROR_DB_SQL;
      }
}


# ==========
# Fatal error
# ==========
sub FatalError
{
  my $error_msg = shift;
  my $error_code = shift;

  Et_Run_Log( $error_msg );
  Et_Error_Log( $error_msg );

  Et_Update_Status($error_code,"Failed Check Error Log");
  et_email::Et_Mail($err_mail_list, "$0 batch job failed", "$0 failed with error message: $error_msg", $mailhost) ;

  exit( $error_code );
}

sub generatebondReqFile 
{
 unless ( open(SFILE_FH,">$global_bond_req_file"))
{
       &FatalError("\nCould not open file [$global_bond_req_file] ",4);
}
&Init();
my $rs;
my $stmt = "set transaction isolation level 0
    select Symbol,Margin_Percent from BondProduct where Marginability_Flag=1 and Margin_Percent > 0";
$rs = $proddb_dbh->prepare($stmt);

 unless ( $rs->execute() ) {
                 &FatalError("generatebondReqFile: Error executing the SQL") ;
        }


while (@rowdata = $rs->fetchrow_array)
        {
        my $REPORT_DATA=sprintf("%s|%s",$rowdata[0],$rowdata[1]);
        print SFILE_FH $REPORT_DATA."\n";
 }
 $rs->finish();
 close SFILE_FH;

}

sub generateEquityReqFile 
{
 unless ( open(SFILE_FH,"<$tpp_eqty_file"))
{
       &FatalError("\nCould not open file [$tpp_eqty_file] ",4);
}

 my $tmp_file="/tmp/temp.txt";

 unless ( open($TMP_FH,">$tmp_file"))
{
       &FatalError("\nCould not open file [$tmp_file] ",4);
}

&Init();
my $rs;
print $TMP_FH $header;
print $TMP_FH $desc;

while(<SFILE_FH>)
{
  chomp($_);
  my @tpp_data = split(/\|/,$_);
  if (($tpp_data[1]  eq  "TPP_EQTY_FILE") or ($tpp_data[0] eq "Symbol"))
  {
    next;
  }

my $stmt = "set transaction isolation level 0
    select max_mpi_price_limit,margin_percent_short from Equity where marginability_flag=1 and margin_percent_short > 0 and symbol = '".$tpp_data[0]."' and exchg_code = '".$tpp_data[4]."'";

$rs = $proddb_dbh->prepare($stmt);

 unless ( $rs->execute() ) {
                 &FatalError("generateEquityReqFile: Error executing the SQL") ;
        }
my @rowdata = $rs->fetchrow_array;
my $REPORT_DATA=$tpp_data[0]."|".$tpp_data[1]."|".$tpp_data[2]."|".$tpp_data[3]."|".$tpp_data[4]."|".$rowdata[1]."|".$rowdata[0];
print $TMP_FH $REPORT_DATA."\n";
        
$rs->finish();
}
print $TMP_FH $trailer;
close SFILE_FH;
close $TMP_FH;
move($tmp_file, $tpp_eqty_file);
}

sub getprevBizDateWpr($)
{
  my $date=shift;
  my $prev_biz_date;
  my $prev_biz_date_mmddyyyy=0;
  my $prev_biz_date_yyyymmdd=0;
  $prev_biz_date =&batch_adp_common_utils::getPrevBizDateFromEndDate($date,1);
  my @outarray=($prev_biz_date =~m/../g);
  $prev_biz_date_mmddyyyy=$outarray[2].$outarray[3].$outarray[0].$outarray[1];
  $prev_biz_date_yyyymmdd=$outarray[0].$outarray[1].$outarray[2].$outarray[3];
  return ($prev_biz_date_mmddyyyy,$prev_biz_date_yyyymmdd);
}
