#!/usr/bin/perl
#######################################
#
# Mysql table audit v 1.0.1 (2010) 
#
# Author Marco Tusa 
# Copyright (C) 2001-2003, 2010
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

#######################################


# TO DO
# Add a valid prefix for all the added fields in the created alter table
#
# Remember to modify the getTableStatus function, it will must accept an array of strings that will be used for skip the non checkable tables
#
#

use visualization;
use commonfunctions;
use mysqldbcommon;
use ConfigIniSimple;

use strict;
use warnings;

#use Tk::DialogBox;

# $dialog = $main->DialogBox( -title   => "Register This Program", -buttons => [ "Register", "Cancel" ] );


use Time::Local;

use DBI;
use Getopt::Long;
$Getopt::Long::ignorecase = 0;


my $html = 0;
my $Param = {};
my $user = '';
my $pass = '';
my $help = undef;
my $host = '' ;
my $outfile;
my $applicationUser=0 ;# 0 = Use current_USER 1 = Use two param from application @UserName @UserHost
my $genericStatus = 2; # 1 = OK go ahead; 2 = Insert again; 0 = exit;
my $finalreport = '';
my @excludelist  ;
my @includelist ;
my @authorizedList ;
my @checkedList;
my @monitoredList;
my $dsn;
my $conf = new ConfigIniSimple;
my $defaultfile;
my $reportmodification = 0; # 0 means false (not doing single check); 1 true run check skipping all others operation


$Param->{user}       = '';
$Param->{password}   = '';
$Param->{host}       = '';
$Param->{port}       = 3306;
$Param->{debug}      = 0;
$Param->{authorized}   = 'root'; 
$Param->{excludelist}  = '';
$Param->{includelist} = '';
$Param->{reset}       = 0;
$Param->{toDelTriggers} ='';
$Param->{doGrants}    = 0;
$Param->{defaultEngine}    = 'InnoDB';
$Param->{defaultBckEngine}    = 'MyIsam';
$Param->{applicationUser}       = 0;
$Param->{monitored} ='';
$Param->{checked} ='';
$Param->{silent}    = 0;
$Param->{keepHistory} = 1;
$Param->{triggerbody} = '';
my $invalidator  = 0 ;

$Param->{appheader}.="\t=============================================================================\n";
$Param->{appheader}.="\t*****************************************************************************\n";
$Param->{appheader}.="\t\t\t\tAUDIT MySQL Procedure\n";
$Param->{appheader}.="\t*****************************************************************************\n";
$Param->{appheader}.="\t=============================================================================\n";
#$Param->{outfile};

################################
#INIZIALIZATION SECTION [START]#
################################
sub init()
{
    if(defined($defaultfile) && $defaultfile ne '' && $Param->{reset} == 0)
    {
        resetSettings($Param);
        
        $conf = loadIniObject($defaultfile);
        my $selected = loadSettingsFromIni($conf,$Param);


        if(defined($selected))
        {
        
            $host = defined($selected->{host})?$selected->{host}:'';
            $Param->{host} = defined($selected->{host})?$selected->{host}:'';
            $Param->{port} = defined($selected->{port})?$selected->{port}:'3306';
            $Param->{user} = defined($selected->{user})?$selected->{user}:'';
            $Param->{outfile} = defined($selected->{outfile})?$selected->{outfile}:undef;
            $outfile = defined($selected->{outfile})?$selected->{outfile}:'';
            $Param->{excludelist} = defined($selected->{excludelist})?$selected->{excludelist}:undef;
            $Param->{includelist} = defined($selected->{includelist})?$selected->{includelist}:undef;
            $Param->{invalidator} = 0;
            $invalidator = 0;
            $Param->{password} = defined($selected->{password})?$selected->{password}:'';
            $Param->{silent} = defined($selected->{silent})?$selected->{silent}:0;
            $Param->{authorized} =defined($selected->{authorized})?$selected->{authorized}:'';    
            $Param->{monitored} =defined($selected->{monitored})?$selected->{monitored}:'';
            $Param->{checked} =defined($selected->{checked})?$selected->{checked}:'';
            $Param->{keepHistory} = defined($selected->{keephistory})?$selected->{keephistory}:1;
            $Param->{applicationUser} = defined($selected->{applicationUser})?$selected->{applicationUser}:0;
        #my %bloks = %{keys(%{$conf})};
        }
        
    }

    $Param->{reset} = 0;
    
    if( $host eq '' )
    {
        $host = getHost();
    }
    $Param->{host} = &URLDecode($host);
    
#    if( $Param->{port} eq '' || $Param->{port} eq '3301')
    if( $Param->{port} eq '')
    {
        $Param->{port} = getPort($Param->{port});
    }
    

    if(defined $outfile && $outfile ne '')
    {
         $Param->{outfile} = URLDecode($outfile);
    }
    else
    {
        $outfile = promptUser("Please insert valid path and file name to use for output script","/tmp/audit.sql","/tmp/audit.sql");
         $Param->{outfile} = URLDecode($outfile);
         open FILEOUT, '>', $Param->{outfile} or die "Couldn't open $Param->{outfile} for writing: $!\n";
         close FILEOUT;
    }
    
    $dsn  = "DBI:mysql:host=$Param->{host};port=$Param->{port}";
    if(defined $Param->{user} && $Param->{user} ne ''){
            $user = "$Param->{user}";
    }
    else
    {
            $user = getUser();
            $Param->{user} = $user;
    }
    
    if(defined $Param->{password} && $Param->{password} ne ''){
            $pass = "$Param->{password}";
    }
    else
    {
            $pass = getPassword();
            $Param->{password} = $pass;
    }
    
    if(defined $Param->{authorized} && $Param->{authorized} eq '' && $Param->{monitored} eq '')
    {
            my $authorized = getAuthorizeFromPrompt($Param->{authorized});
            $Param->{authorized} = $authorized;
    }
    
    
    
    if( defined $Param->{outfile}){
    
        if (open FILEOUT, '>', $Param->{outfile}){
        }
    }
    
    #my $filexxx;
    #
    #if( defined $Param->{outfile}){
    #
    #    if (open ($filexxx, '>', $Param->{outfile})){
    #    }
    #    print $filexxx "\n aaaaaaa \n";
    #    close $filexxx;
    #}
    
    $Param->{invalidator} = $invalidator;
    
    $Param = getExcludeList($Param);
    $Param = getIncludeList($Param);
    
    $invalidator = $Param->{invalidator};
    while($invalidator > 1 || $invalidator ==0)
    {
        $Param = checkForLists($Param);
        $invalidator = $Param->{invalidator};    
        
    }
    
    
    
}
################################
#INIZIALIZATION SECTION [ENDS]#
################################

    

sub getInitialOk($)
{
    $Param = shift;
    if($Param->{silent} == 1)
    {
        return 1;    
    }
    
    system('clear');
    
    my $reportString = $Param->{appheader};
    $reportString .= "\t=============================================================================\n";
    $reportString .= "\t\t\t\t Procedure Settings\n";
    $reportString .= "\t=============================================================================\n";
    $reportString .= "\tHost = ".$Param->{host}."\n";
    $reportString .= "\tPort = ".$Param->{port}."\n";
    $reportString .= "\tUser = ".$Param->{user}."\n";
    $reportString .= "\t------------------------\n";
    $reportString .= "\tSQL generated file = ".$Param->{outfile}."\n";
    if(defined($Param->{excludelist}) && $Param->{excludelist} ne ''){
        $reportString .= "\tExclude Schema list = ".getArrayAsComma($Param->{excludelist})."\n";
    }
    if(defined($Param->{includelist}) && $Param->{includelist} ne ''){
        $reportString .= "\tInclude Schema List = ".getArrayAsComma($Param->{includelist})."\n";
    }
    $reportString .= "\tNot traced user(s) = ".$Param->{authorized}."\n";
    $reportString .= "\tMonitored user(s) = ".$Param->{monitored}."\n";
    $reportString .= "\tChecked user(s) = ".$Param->{checked}."\n";
    $reportString .= "\t=============================================================================\n";
    
    print $reportString;
    
    my $question .= "\tChoose:\n";
    $question .= "\t[1] Continue\n";
    $question .= "\t[2] Re insert all values\n\n";
    $question .= "\t[S] Save settings to file\n";
    $question .= "\t[0] EXIT\n";
    $question .= "\t=============================================================================\n";
    
    $genericStatus = promptUser($question,1,"1(default)|2|0");
    print "\n".$genericStatus;
    if($genericStatus eq '0')
    {
        exit(0);
    }
        
    return $genericStatus;
    
}

sub resetSettings($)
{
    $Param = shift;
    $host = '';
    $Param->{host} = '';
    $Param->{port} = '3306';
    $Param->{user} = '';
    undef($Param->{outfile});
    $outfile = '';
    undef($Param->{excludelist});
    undef($Param->{includelist});
    $Param->{invalidator} = 0;
    $invalidator = 0;
    $Param->{password} = '';
    $Param->{authorized} ='root';    
    $_=0;
    close FILEOUT;
    
    return $Param;
}


if (
    !GetOptions(
        'user|u:s'       => \$Param->{user},
        'password|p:s'   => \$Param->{password},
        'host|H:s'       => \$host,
        'port|P:i'       => \$Param->{port},
        'authorized|a:s'   => \$Param->{authorized},
        'outfile|o:s'    => \$outfile,
        'debug|e:i'      => \$Param->{debug},
        'excludelist|x:s' => \$Param->{excludelist},
        'includelist|i:s' => \$Param->{includelist},
        'keephistory|i'  =>\$Param->{keepHistory},
        'silent:i'        =>\$Param->{silent},
        'help|h:s'       => \$help,
        'defaults-file:s'=>\$defaultfile,
        'reportmodification:i' =>\$reportmodification,

    )
  )
{
    ShowOptions();
    exit(0);
}
else{
    
    if(defined $reportmodification && $reportmodification ne ''){
        
        $Param->{reportmodification} = $reportmodification;
    }
    
    if(defined $help)
    {
        ShowOptions();
        exit(0);
    }
    
    init();
    
    if( defined $Param->{reportmodification}  && $Param->{reportmodification} eq '0')
    {
        while($genericStatus eq '2')
        {
            
            $genericStatus = getInitialOk($Param);
            if($genericStatus eq '0')
            {
                exit(0);            
            }
            elsif($genericStatus eq '2')
            {
                init();
                $Param = resetSettings($Param);
                $Param->{reset} = 2;
                
            }
            elsif($genericStatus eq 'S')
            {
                my $selected = {};
                my $defaultFile = promptUser("Defaults file",$defaultfile,$defaultfile);
                my $settingsName = promptUser("Settings name ",'','');
    
                $selected->{host} = $Param->{host};
                $selected->{port} = $Param->{port};
                $selected->{user} = $Param->{user};
                $selected->{outfile} = $Param->{outfile};
                $selected->{excludelist} = defined($Param->{excludelist})?getArrayAsComma($Param->{excludelist}):'';
                $selected->{includelist} = defined($Param->{includelist})?getArrayAsComma($Param->{includelist}):'' ;
                $selected->{password} = $Param->{password};
                $selected->{authorized} = $Param->{authorized};    
                
                saveSettings($selected,$defaultFile,$settingsName);
                $genericStatus=2;
                $genericStatus = getInitialOk($Param);
                
            }
            elsif($genericStatus eq '1')
            {
                print "\n";
                print "\nSTART Process.";
                print "\n";
            }
            else
            {
                $genericStatus = getInitialOk($Param);
            }
            
        }
    }
    else
    {
        print "\n ########################### STARTING Schemas Check ####################";
    }
}

#if ( !defined $help && $help ne '' ) {
#    ShowOptions();
#    exit(0);
#}

if($Param->{debug}){
    debugEnv();
}

#
#my $username = &promptUser("Enter the username ","AA");
#print "Name= ".$username."\n";



##############################################################################################
# F U N C T I O N  S E C T I O N
# T O  B E C O M E S  O B J E C T  M E T H O D S
##############################################################################################

sub getAuthorizeFromPrompt($)
{
    my $authorized = shift;
    
    return $authorized = promptUser("Please type list comma separated\n of user that should *not* be traced ",$authorized,$authorized);
}

sub getMonitoredFromPrompt($)
{
    my $monitored = shift;
    
    return $monitored = promptUser("Please type list comma separated\n of user that should be traced ",$monitored,$monitored);
}

sub getCheckedFromPrompt($)
{
    my $checked = shift;
    
    return $checked = promptUser("Please type list comma separated\n of user that should be Checked ",$checked,$checked);
}


sub getAuthorizedListSQL($)
{
    my $Param = shift ;  
 
    if((defined $Param->{authorized}) and ($Param->{authorized} ne "")){
       @authorizedList = split(/,/,join(',',$Param->{authorized}));
       $Param->{authorizedList}=\@authorizedList;
       #$Param->{authorized}=${getAuthorizedList($Param)};
    }
    else
    {
        undef $Param->{authorizedList};
  #      undef $Param->{authorized};
    
    }
 
 
 
 #   my @authorizedList  = @{$Param->{authorizedList}};
    my $strUserList = "";
    
    if(defined $Param->{authorizedList})
       {
	    for (my $i=0; $i < @authorizedList; $i++) {
		if ($authorizedList[$i] ne "") {
                    if($i > 0)
                    {
                        $strUserList = $strUserList." AND \@username NOT LIKE \"$authorizedList[$i]\"";
                    }
                    else
                    {
                        $strUserList = " \@username NOT LIKE \"$authorizedList[$i]\"";
                    }
                    
		}
		
	    }
            $strUserList = $strUserList. " AND \@username NOT LIKE '' "
            
	    
       }
    return \$strUserList;
}

sub getMonitoredListSQL($)
{
    my $Param = shift ;
    
    if((defined $Param->{monitored}) and ($Param->{monitored} ne "")){
       @monitoredList = split(/,/,join(',',$Param->{monitored}));
       $Param->{monitoredList}=\@monitoredList;
       #$Param->{authorized}=${getAuthorizedList($Param)};
    }
    else
    {
 #       undef $Param->{monitored};
        undef $Param->{monitoredList};
    
    }    
    
#    my @monitoredList  = @{$Param->{monitoredList}};
    my $strUserList = "";
    
    if(defined $Param->{monitoredList})
       {
	    for (my $i=0; $i < @monitoredList; $i++) {
		if ($monitoredList[$i] ne "") {
                    if($i > 0)
                    {
                        $strUserList = $strUserList." OR \@username LIKE \"$monitoredList[$i]\"";
                    }
                    else
                    {
                        $strUserList = " \@username  LIKE \"$monitoredList[$i]\"";
                    }
                    
		}
		
	    }
            $strUserList = $strUserList. " OR \@username LIKE '' "
            
	    
       }
    return \$strUserList;
}

sub getCheckedListSQL($)
{
    my $Param = shift ;

    if((defined $Param->{checked}) and ($Param->{checked} ne "")){
       @checkedList = split(/,/,join(',',$Param->{checked}));
       $Param->{checkedList}=\@checkedList;
       #$Param->{authorized}=${getAuthorizedList($Param)};
    }
    else
    {
#        undef $Param->{checked};
        undef $Param->{checkedList};
    
    }        
    
 #   my @checkedList  = @{$Param->{checkedList}};
    my $strUserList = "";
    
    if(defined $Param->{checkedList})
       {
	    for (my $i=0; $i < @checkedList; $i++) {
		if ($checkedList[$i] ne "") {
                    if($i > 0)
                    {
                        $strUserList = $strUserList." OR \@username LIKE \"$checkedList[$i]\"";
                    }
                    else
                    {
                        $strUserList = " \@username LIKE \"$checkedList[$i]\"";
                    }
                    
		}
		
	    }
            $strUserList = $strUserList. " OR \@username LIKE '' "
            
	    
       }
    return \$strUserList;
}


sub createAuditSysTable($)
{
    my $Param = shift ;  
    my $dbh  = $Param->{dbh};
    
    print "\n";
    print " --- === Security schema section === --- [START]";
    print "\n";
    
    my $cmd = "
    /*!40101 SET \@OLD_SQL_MODE=\@\@SQL_MODE */;
    /*!40014 SET \@OLD_UNIQUE_CHECKS=\@\@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
    /*!40014 SET \@OLD_FOREIGN_KEY_CHECKS=\@\@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
    /*!40111 SET \@OLD_SQL_NOTES=\@\@SQL_NOTES, SQL_NOTES=0 */;";

    if (defined($Param->{outfile}))
    {
        print FILEOUT "\n", $cmd ," \n";
    }
    
    my $aUser= $Param->{user};
    my $gHost=$Param->{host};
    my $gPassword= $Param->{password};
    my $gGrants = 'ALL';
    my $sth;
    
    if(defined $Param->{doGrants} && $Param->{doGrants} == 1)
    {
        $cmd = "GRANT $gGrants on security.* to $aUser\@'$gHost' identified by '$gPassword';";
        $sth = $dbh->prepare($cmd);
        $sth->execute();
    }

    $cmd = "create database if not exists security;";
    
    $sth = $dbh->prepare($cmd);
    $sth->execute();

    if (defined($Param->{outfile}))
    {
        print FILEOUT "\n", $cmd ," \n";
    }

            #`SYS_HISTOR_ID` CHAR(36) NOT NULL ,
    $cmd = "create table if not exists `security`.`SYS_AUDIT_MAINLOG`(
            `SYS_ID` bigint Unsigned AUTO_INCREMENT NOT NULL ,
            `SYS_HISTOR_SCHEMA` varchar(50) collate utf8_bin NOT NULL default '',
            `SYS_HISTOR_TABLE` varchar(200) collate utf8_bin default NULL,
            `SYS_HISTOR_OPERATION_TYPE` varchar(6) collate utf8_bin default NULL,
            `SYS_HISTOR_DAY` mediumint Unsigned NOT NULL,
            `SYS_HISTOR_TIMESTAMP` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            `SYS_HISTOR_USER` varchar(20) collate utf8_bin default NULL,
            `SYS_HISTOR_TERM` varchar(50) collate utf8_bin default NULL,
            `SYS_HISTOR_APPID` varchar(20) collate utf8_bin default NULL,
            `SYS_HISTOR_APPIP` varchar(50) collate utf8_bin default NULL,
            `SYS_HISTOR_SESSION_ID` decimal(21,0) default NULL,
            `SYS_HISTORY_AUDIT_TYPE` char(1) default NULL,
            PRIMARY KEY  (`SYS_ID`,`SYS_HISTOR_DAY`),
            INDEX `IDX_SCHEMA_TAB` (`SYS_HISTOR_SCHEMA`,`SYS_HISTOR_TABLE`)
          ) ENGINE=innodb ;\n";
          
#    $cmd = $cmd."/*!51001 ADD PARTITION COMMANDS */";      
          
    #$sth = $dbh->prepare($cmd);
    #$sth->execute();
    
    if (defined($Param->{outfile}))
    {
        print FILEOUT "\n", $cmd ," \n";
    }

    
    print "\n";
    print " --- === Security schema section === --- [END]";
    print "\n";
    
    return $Param;
}





    

 
sub createAuditByTable($)
{
      my $Param = shift;  
      my $dbh  = $Param->{dbh};
      my %databases = %{$Param->{tables}};

    print "\n";
    print " --- === AUDIT creation Section === --- [START]";
    print "\n";

    $Param->{authorizedListSQL} = getAuthorizedListSQL($Param);
    $Param->{monitoredListSQL} = getMonitoredListSQL($Param);
    $Param->{checkedListSQL} = getCheckedListSQL($Param);
    #if((defined $Param->{authorized}) and ($Param->{authorized} ne "")){
    #   @authorizedList = split(/,/,join(',',$Param->{authorized}));
    #   $Param->{authorizedList}=\@authorizedList;
    #   $Param->{authorized}=${getAuthorizedList($Param)};
    #}
    #else
    #{
    #    undef $Param->{authorizedList};
    #    undef $Param->{authorized};
    #
    #}
      

        my $alltables = "";
        my %locReport;

    foreach my $key (sort keys %databases)
    {
        my @tables = @{$databases{$key}};
        my $dimTable = '';

        my $cmd = "use ".$key;
        my $sth = $dbh->do($cmd);
        my $time = localtime;
	
        my %columnHash ;
        my @columTypeDim ;
	my $tblDirty = 0;
        my $audDb = $key."_audit";
	
        #if (defined($Param->{outfile})){
        #    print FILEOUT "Starting on ", $time," \n";
        #}
        print "processing = $key [start] at $time \n";
        if (defined($Param->{outfile}))
         {
             print FILEOUT "\n", $cmd ,"; \n";
             $cmd = "";
             #add grants for each create db to the user which is running the application
             my $aUser= $Param->{user};
             my $gHost=$Param->{host};
             my $gPassword= $Param->{password};
             my $gGrants = 'ALL';
             
             if(defined $Param->{doGrants} && $Param->{doGrants} == 1)
             {
                 $cmd = "GRANT $gGrants on $audDb.* to $aUser\@'$gHost' identified by '$gPassword';";
             }
             $cmd = $cmd."\ncreate database if not exists $audDb;";
             print FILEOUT "\n", $cmd ," \n";
         }

        for (my $icounter = 0 ; $icounter <= $#tables; $icounter++)
        {
            my %columnHash ;
            

	    $dimTable = $tables[$icounter];
            
            #get table number of rows
            my $numberOfRows;
            
            my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
            $mon = ++$mon;
            $year = $year+=1900;

#            print $audDb_bck . "\n";
           

#            $cmd = "SELECT COLUMN_NAME FROM information_schema.COLUMNS where table_schema='$key' and table_name='$dimTable'";
            $cmd = "SELECT COLUMN_NAME, Data_TYPE, NUMERIC_PRECISION, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS where table_schema='$key' and table_name='$dimTable'";

	    $sth = $dbh->prepare($cmd);
	    $sth->execute();

	    #my $recordSet = $sth->fetchrow_hashref();
	    
	    
	    #if (defined($recordSet))
	    #{
	      my $row = undef;
              my $newColumns = "";  
              my $oldColumns = "";
              
	      while ( $row = $sth->fetchrow_hashref() )
	      {
                my @columTypeDim ;
		$newColumns = $newColumns."NEW\.".$row->{'COLUMN_NAME'}.",";
		$oldColumns = $oldColumns."OLD\.".$row->{'COLUMN_NAME'}.",";
                
                $columTypeDim[0] = $row->{'Data_TYPE'};
                $columTypeDim[1] = $row->{'NUMERIC_PRECISION'};
                $columTypeDim[2] = $row->{'CHARACTER_MAXIMUM_LENGTH'};
                $columnHash{$row->{'COLUMN_NAME'}} = \@columTypeDim
	      }
	    #}
	    $sth->finish();

            $Param->{currentDb} = $key;
            $Param->{currentTable} = $dimTable;
            
            $Param->{tbColumnStatus} = \%columnHash;
            
            $tblDirty=0;
            $tblDirty = getTableStatus($Param);
            #0 = stay as it is ; 1 do backup ; 2 generate audit table;
            
            if($tblDirty == 0)
            {
                print $dimTable." [NOT MODIFIED]\n";
            }
            elsif($tblDirty == 1)
            {
                print $dimTable." [MODIFIED Table will be BACKUP with name(".$audDb.".".$dimTable."_".$hour."_".$min."_"."_".$mday."_".$mon."_".$year.")]\n";
                print "\t ".$Param->{dirtyMessage};
            }
            elsif($tblDirty == 2)
            {
                print $dimTable." [NEW AUDIT on table ".$audDb." Table will be CREATED ]\n";
            }


            
            undef($Param->{tbColumns});
            
            if($tblDirty != 0 )
            {

                if($tblDirty == 1 )
                {
                    my $audDb_bck = $audDb.".".$dimTable."_".$hour."_".$min."_"."_".$mday."_".$mon."_".$year;

                    $cmd = "RENAME TABLE ".$audDb.".".$dimTable." TO ".$audDb_bck.";\n";
                    $cmd = $cmd."ALTER TABLE ".$audDb_bck." engine=$Param->{defaultBckEngine};\n";

                    if (defined($Param->{outfile}))
                    {
                        print FILEOUT "\n", $cmd ," \n";
                    }                    
                    
                }
        
                
                # Create Table for audit in same db same name  with security_aud postfix
                my $currentTableAud = $audDb.".".$dimTable;
                $cmd = "CREATE TABLE IF NOT EXISTS $currentTableAud LIKE $key.$dimTable;";
                #$cmd = "CREATE TABLE IF NOT EXISTS $currentTableAud select * from $key.$dimTable limit 0;";
                #$sth = $dbh->prepare($cmd);
                #$sth->execute();
                #select distinct CONSTRAINT_NAME,CONSTRAINT_SCHEMA,TABLE_NAME from information_schema.key_column_usage where TABLE_SCHEMA='test' and TABLE_NAME='tbtest_child11';
                #select column_name,table_schema,table_name from information_schema.columns where table_schema='test' and table_name='aa' and extra='auto_increment';
                
                if (defined($Param->{outfile}))
                {
                    print FILEOUT "\n", $cmd ," \n";
                }

                my $autoincrement = getAutoincrement($Param,$key,$dimTable);
                if( defined $autoincrement && $autoincrement ne "" )
                {
                    $cmd = "ALTER TABLE $currentTableAud MODIFY $autoincrement not null;";
                    if (defined($Param->{outfile})){print FILEOUT "\n", $cmd ," \n";}
                }
                


                my @Index= getIndex($Param,$key,$dimTable);
                
                my $boundary = $#Index;
                my %tableHashMap ;
    
                $cmd = "ALTER TABLE $currentTableAud ";
                for( my $counter = 0 ; $counter <= $boundary; $counter++ )
                {
                    if($counter < 1){
                        $cmd = $cmd."Drop ";
                    }
                    else
                    {
                        $cmd = $cmd.", Drop ";
                    }
                    if($Index[$counter] eq "PRIMARY"){
                        $cmd = $cmd." PRIMARY KEY ";
                    }
                    else
                    {
                        $cmd = $cmd." KEY `$Index[$counter]`";
                    }
                    
                }
                 $cmd = $cmd.";";
                if (defined($Param->{outfile}))
                {
                    print FILEOUT "\n", $cmd ," \n";
                }
                 
    
                #$cmd = "alter TABLE $currentTableAud add (AUDIT_operation varchar(10), AUDIT_idlog bigint, AUDIT_idmainlog char(36), AUDIT_recordtype char(3), AUDIT_audittype char(1));\n";    
                $cmd = "alter TABLE $currentTableAud add ( 
                    AUDIT_operation varchar(10),
                    AUDIT_idlog bigint Unsigned NOT NULL,
                    AUDIT_recordtype char(3),
                    AUDIT_audittype char(1),
                    AUDIT_day mediumint Unsigned NOT NULL,
                    AUDIT_counter bigint Unsigned AUTO_INCREMENT NOT NULL,
                    PRIMARY KEY (`AUDIT_counter`,`AUDIT_day`),
                    INDEX `IDX_AUDIT_IDLOG` (`AUDIT_idlog`)
                );\n";
                #$cmd = $cmd."alter TABLE $currentTableAud add PRIMARY KEY (`AUDIT_counter`,`AUDIT_day`), add INDEX `IDX_AUDIT_IDLOG` (`AUDIT_idlog`);";
                $cmd = $cmd."\nalter TABLE $currentTableAud ENGINE=".$Param->{defaultEngine}.";";

                #$sth = $dbh->prepare($cmd);
                #$sth->execute();
    
                if (defined($Param->{outfile}))
                {
                    print FILEOUT "\n", $cmd ," \n";
                }
                $cmd = "";
    #!!!!!! CONTROLLARE 
                #$cmd = "alter TABLE $currentTableAud engine=myisam;";
                #if (defined($Param->{outfile}))
                #{
                #    print FILEOUT "\n", $cmd ," \n";
                #}
            
            }

#	    $sth = $dbh->prepare($cmd);
#	    $sth->execute();
#            $sth->finish();
            
            $Param->{currentdb}=$key;
            $Param->{audDb}=$audDb;
            $Param->{currentTable}=$dimTable;
            $Param->{currentNew}=$newColumns;
            $Param->{currentOld}=$oldColumns;
            
            #do the triggers
            if($tblDirty != 0 ){
                createDeleteTrigger($Param);
                createUpdateTrigger($Param);
                createInsertTrigger($Param);
            }    
            
        }

        $time = localtime;
        #if (defined($Param->{outfile})){
        #    print FILEOUT "\n\nClosing on", $time," \n";
        #}
        print "\n\nprocessing = $key [end] at $time \n";
        #$sth = $dbh->do($cmd);
    }
        
        #my $currDb = $databases[$counter];
        #my $cmd = "use ".$currDb;
        
        
        my $cmd = "/*!40101 SET SQL_MODE=\@OLD_SQL_MODE */;
                /*!40014 SET FOREIGN_KEY_CHECKS=\@OLD_FOREIGN_KEY_CHECKS */;
                /*!40014 SET UNIQUE_CHECKS=\@OLD_UNIQUE_CHECKS */;";

        #my $toDelTriggers = "  /*======================UNCOMMENT THIS SECTION TO REMOVE ALL THE CREATED TRIGGERS==================*/\n";
        #$toDelTriggers = $toDelTriggers.$Param->{toDelTriggers};
        #$cmd = $cmd."\n".$toDelTriggers." \n /* ### /*";
        #
        #if (defined($Param->{outfile}))
        #{
        #    print FILEOUT "\n", $cmd ," \n";
        #}
        #if (defined($Param->{outfile}) &&  $Param->{triggerbody} ne '')
        #{
        #    print FILEOUT "\n /*============================= TRIGGER BODY SECTION ======================================*/\n";
        #    print FILEOUT "/*".$Param->{triggerbody}."*/ \n";
        #}

        
    print "\n";
    print " --- === AUDIT creation Section === --- [END]";
    print "\n";

    # Section for the Trigger notification


        
    return \$Param;
}


sub createDeleteTrigger($)
{

    my $Param = shift;
    
    my $db = $Param->{currentdb};
    my $table = $Param->{currentTable};
    my $useraut = ${$Param->{authorizedListSQL}};
    my $usermon = ${$Param->{monitoredListSQL}};
    my $usercheck = ${$Param->{checkedListSQL}};    
    my $keepHistory = $Param->{keepHistory};
    my $userToWrite;
    
    my $new = $Param->{currentNew};
    my $old = $Param->{currentOld};
    my $audDb = $Param->{audDb};
    my $toDelTriggers = $Param->{toDelTriggers};
    
    my $TriggerGet = {};
    
    my $fullTablename = $db.".".$table;
    
    if (defined($Param->{outfile}))
    {
        print FILEOUT "\n", "-- Delete Trigger for $table  [START]"," \n";
    }

    $Param->{methodToAudit} = "DELETE";
    
    $TriggerGet = triggerBodyReplace($Param);
    my $cmd ="";
    my $acstatment = $Param->{acstatment};
    my $existingTriggerName = $Param->{existingTriggerName};

    $toDelTriggers = $toDelTriggers."\n"."/* Drop trigger if exists $db\.aud_d_$table; /*";
    $Param->{toDelTriggers}=$toDelTriggers ;
    
    $cmd = "DELIMITER ##    
    ";
    if(defined $acstatment &&  $acstatment ne "")
    {
        $cmd = $cmd."\t /* WARNING EXISTING TRIGGER FOUND\n";
        $cmd = $cmd."\t TRIGGER NAME = $db\.$existingTriggerName\n";
        $cmd = $cmd."\t GOING TO DROP IT AND INCLUDE IN THE NEW ONE*/\n";
        $cmd = $cmd."\t /* ADDING Trigger definition in in the Trigger Section*/ \n";
        
        $cmd = $cmd." Drop trigger if exists $db\.$existingTriggerName##
        ";
        $Param->{triggerbody} = $Param->{triggerbody}." \n /* SCHEMA = $db \n TABLE = $table \n TRIGGER_NAME= $existingTriggerName \n TRIGGER_ACTION=DELETE \n";
        $Param->{triggerbody} = $Param->{triggerbody}."TRIGGER_BODY= $acstatment \n";
    }
    
    if( defined $useraut && $useraut ne ""){
        $userToWrite = $useraut;
    }
    elsif ( defined $usermon && $useraut eq "" ){
        $userToWrite = $usermon;
    }
    else
    {
        $userToWrite = $useraut;
    }

    if ( defined $usercheck && $usercheck ne "" && $useraut eq "" ){
    }
    else
    {
        $usercheck = '';
    }


    $cmd = $cmd." Drop trigger if exists $db\.aud_d_$table##
            
            CREATE TRIGGER $db\.aud_d_$table
             BEFORE DELETE ON $fullTablename FOR EACH ROW
             BEGIN
                    DECLARE sqlcode INT DEFAULT 0;
                    DECLARE CONTINUE HANDLER FOR 1054 SET sqlcode = 1054; 
                    DECLARE CONTINUE HANDLER FOR 1136 SET sqlcode = 1136;

                    SET \@username='';
                    SET \@action='DELETE';
                    SET \@host='';
                    SET \@connectionid=0;
                    SET \@appID='';
                    SET \@appIP='';
                    SET \@TODAY=0;
                    SET \@LASTINSERT=0;
                    
                    
                    SELECT TO_DAYS(NOW())into \@TODAY;
";
    if ($Param->{applicationUser} eq 0){
                    
    $cmd = $cmd."
                    SELECT SUBSTRING_INDEX(USER(),'\@',1) INTO \@username;
                    SELECT SUBSTRING_INDEX(USER(),'\@',-1) INTO \@host;
                    SELECT connection_id() into \@connectionid;
";
    }
    else{
    $cmd = $cmd."
                if (SELECT \@UserAppID IS NOT NULL) then
                    SELECT SUBSTRING_INDEX(USER(),'\@',1) INTO \@username;
                    SELECT SUBSTRING_INDEX(USER(),'\@',-1) INTO \@host;
                    SELECT \@UserAppID INTO \@appID;
                    SELECT \@UserAppIP INTO \@appIP;
                    SELECT connection_id() into \@connectionid;
            
                else              
                    SELECT SUBSTRING_INDEX(USER(),'\@',1) INTO \@username;
                    SELECT SUBSTRING_INDEX(USER(),'\@',-1) INTO \@host;
                    SELECT connection_id() into \@connectionid;            
                end if;\n
";
    }
    $cmd = $cmd."        
                    
                    if ($userToWrite) then
                    ";
                        $cmd = $cmd.printSYS_AUDIT_MAINLOG($db,$fullTablename,'A');
                        $cmd = $cmd."\n  SELECT LAST_INSERT_ID() INTO \@LASTINSERT; \n";
                    
                        if($keepHistory gt '0' )
                        {
                            $cmd = $cmd.printHistoryInsert($audDb,$table,$old,'A');
                        }          

                    $cmd = $cmd. "\n
                    end if;\n";
                    
                            #this block insert data if a list od user to check exists;
                            if(defined $usercheck && $usercheck ne "")
                            {
                                $cmd = $cmd."\n -- check user audit \n";
                                $cmd = $cmd."
                    if ($usercheck ) then
                    
                    ";
                        $cmd = $cmd.printSYS_AUDIT_MAINLOG($db,$fullTablename,'A');
                    
                        if($keepHistory gt '0' )
                        {
                            $cmd = $cmd.printHistoryInsert($audDb,$table,$old,'A');
                        }          

                    $cmd = $cmd. "\n
                    end if;\n";
                            }
                            

                            #This block reports the previously existing triggers in to the new one [START]
                            if(defined $acstatment &&  $acstatment ne "")
                            {
                                $cmd = $cmd."\n".$existingTriggerName.":   ".$acstatment." ".$existingTriggerName.";"
                            }
                            #This block reports the previously existing triggers in to the new one [END]

                        $cmd = $cmd ."\n ";

            $cmd = $cmd ."
            END##

            DELIMITER ;";
            
        if (defined($Param->{outfile})){
            
            print FILEOUT "\n", $cmd," \n";
            print FILEOUT "\n", "-- Delete Trigger for $table  [END]"," \n";
            
        }
            
            
    return \$Param;
}

sub createUpdateTrigger($)
{

    my $Param = shift;
    
    my $db = $Param->{currentdb};
    my $table = $Param->{currentTable};
    my $new = $Param->{currentNew};
    my $old = $Param->{currentOld};
    my $audDb = $Param->{audDb};
    my $toDelTriggers = $Param->{toDelTriggers};
    my $useraut = ${$Param->{authorizedListSQL}};
    my $usermon = ${$Param->{monitoredListSQL}};
    my $usercheck = ${$Param->{checkedListSQL}};    
    my $keepHistory = $Param->{keepHistory};
    
    my $userToWrite;    
    
    my $TriggerGet = {};
    
    my $fullTablename = $db.".".$table;
    
    
    
    if (defined($Param->{outfile}))
    {
        print FILEOUT "\n", "-- Update Trigger for $table  [START]"," \n";
    }

       $Param->{methodToAudit} = "UPDATE";
    
    $TriggerGet = triggerBodyReplace($Param);
    my $cmd ="";
    my $acstatment = $Param->{acstatment};
    my $existingTriggerName = $Param->{existingTriggerName};
  
    $toDelTriggers = $toDelTriggers."\n"."/* Drop trigger if exists $db\.aud_u_$table; /*";
    $Param->{toDelTriggers}=$toDelTriggers ;
    
    if( defined $useraut && $useraut ne ""){
        $userToWrite = $useraut;
    }
    elsif ( defined $usermon && $useraut eq "" ){
        $userToWrite = $usermon;
    }
    else
    {
        $userToWrite = $useraut;
    }
    
    if ( defined $usercheck && $usercheck ne "" && $useraut eq "" ){
    }
    else
    {
        $usercheck = '';
    }
    
    $cmd = "DELIMITER ##
    
    ";
    if(defined $acstatment &&  $acstatment ne "")
    {
        $cmd = $cmd."\t /* WARNING EXISTING TRIGGER FOUND\n";
        $cmd = $cmd."\t TRIGGER NAME = $db\.$existingTriggerName\n";
        $cmd = $cmd."\t GOING TO DROP IT AND INCLUDE IN THE NEW ONE*/\n";
        $cmd = $cmd."\t /* ADDING Trigger definition in in the Trigger Section*/ \n";
        
        $cmd = $cmd." Drop trigger if exists $db\.$existingTriggerName##
        ";
        $Param->{triggerbody} = $Param->{triggerbody}." \n/* SCHEMA = $db\n TABLE = $table\n TRIGGER_NAME= $existingTriggerName\n TRIGGER_ACTION=UPDATE \n";
        $Param->{triggerbody} = $Param->{triggerbody}."TRIGGER_BODY= $acstatment\n";
        
    }
    $cmd = $cmd." Drop trigger if exists $db\.aud_u_$table##
            
            CREATE TRIGGER $db\.aud_u_$table
             BEFORE UPDATE ON $fullTablename FOR EACH ROW
             BEGIN
                    DECLARE sqlcode INT DEFAULT 0;
                    DECLARE CONTINUE HANDLER FOR 1054 SET sqlcode = 1054; 
                    DECLARE CONTINUE HANDLER FOR 1136 SET sqlcode = 1136;

                    SET \@username='';
                    SET \@action='UPDATE';
                    SET \@host='';
                    SET \@connectionid=0;
                    SET \@appID='';
                    SET \@appIP='';
                    SET \@TODAY=0;
                    SET \@LASTINSERT=0;
                    
                    SELECT TO_DAYS(NOW())into \@TODAY;
                    

            
                     -- set \@counter = \@counter+1;
                    
";
    if ($Param->{applicationUser} eq 0){
                    
    $cmd = $cmd."
                    SELECT SUBSTRING_INDEX(USER(),'\@',1) INTO \@username;
                    SELECT SUBSTRING_INDEX(USER(),'\@',-1) INTO \@host;
                    SELECT connection_id() into \@connectionid;
";
    }
    else{
    $cmd = $cmd."
                if (SELECT \@UserAppID IS NOT NULL) then
                    SELECT SUBSTRING_INDEX(USER(),'\@',1) INTO \@username;
                    SELECT SUBSTRING_INDEX(USER(),'\@',-1) INTO \@host;
                    SELECT \@UserAppID INTO \@appID;
                    SELECT \@UserAppIP INTO \@appIP;
                    SELECT connection_id() into \@connectionid;
                
                else              
                    SELECT SUBSTRING_INDEX(USER(),'\@',1) INTO \@username;
                    SELECT SUBSTRING_INDEX(USER(),'\@',-1) INTO \@host;
                    SELECT connection_id() into \@connectionid;            
                end if;\n
";
    }
    $cmd = $cmd."        
            
                    
                    if ($userToWrite) then
                     ";
                        $cmd = $cmd.printSYS_AUDIT_MAINLOG($db,$fullTablename,'A');
                        $cmd = $cmd."\n  SELECT LAST_INSERT_ID() INTO \@LASTINSERT; \n";                        
                    
                        if($keepHistory gt '0' )
                        {
                            $cmd = $cmd.printHistoryInsert($audDb,$table,$old,'A');
                        }          

                    $cmd = $cmd. "\n
                    end if;\n";
                    
                            #this block insert data if a list od user to check exists;
                            if(defined $usercheck && $usercheck ne "")
                            {
                                $cmd = $cmd."\n -- check user audit \n";
                                $cmd = $cmd."
                    if ($usercheck) then
                     ";
                        $cmd = $cmd.printSYS_AUDIT_MAINLOG($db,$fullTablename,'A');
                    
                        if($keepHistory gt '0' )
                        {
                            $cmd = $cmd.printHistoryInsert($audDb,$table,$old,'A');
                        }          

                    $cmd = $cmd. "\n
                    end if;\n";
                            }
                                                
                            #This block reports the previously existing triggers in to the new one [START]
                            if(defined $acstatment &&  $acstatment ne "")
                            {
                                $cmd = $cmd."\n".$existingTriggerName.":   ".$acstatment." ".$existingTriggerName.";"
                            }
                            #This block reports the previously existing triggers in to the new one [END]

                if($keepHistory gt '0' )
                {                                
                            $cmd = $cmd ."\n
                    if ($userToWrite) then
                            INSERT INTO $audDb.$table values($new\@action,\@LASTINSERT,'NEW','A',\@TODAY,NULL);
                    end if;";
                            if(defined $usercheck && $usercheck ne "")
                            {
                                $cmd = $cmd."\n";
                                $cmd = $cmd."
                    if ($usercheck) then
                            INSERT INTO $audDb.$table values($new\@action,\@LASTINSERT,'NEW','C',\@TODAY,NULL);
                    end if;
                    ";
                            };
                };
                            
            
            $cmd = $cmd ."
            END##

            DELIMITER ;";
            
        if (defined($Param->{outfile})){
            
            print FILEOUT "\n", $cmd," \n";
            print FILEOUT "\n", "-- UPDATE Trigger for $table  [END]"," \n";
            
        }
            
            
    return \$Param;
}


sub createInsertTrigger($)
{
    my $Param = shift;
    
    my $db = $Param->{currentdb};
    my $table = $Param->{currentTable};
    my $new = $Param->{currentNew};
    my $old = $Param->{currentOld};
    my $audDb = $Param->{audDb};
    my $toDelTriggers = $Param->{toDelTriggers};
    my $useraut = ${$Param->{authorizedListSQL}};
    my $usermon = ${$Param->{monitoredListSQL}};
    my $usercheck = ${$Param->{checkedListSQL}};        
    my $keepHistory = $Param->{keepHistory};
    
    my $userToWrite;        
    my $TriggerGet = {}; 
    
    my $fullTablename = $db.".".$table;
    
    if (defined($Param->{outfile}))
    {
        print FILEOUT "\n", "-- Insert Trigger for $table  [START]"," \n";
    }

   $Param->{methodToAudit} = "INSERT";
    
    $TriggerGet = triggerBodyReplace($Param);
    my $cmd ="";
    my $acstatment = $Param->{acstatment};
    my $existingTriggerName = $Param->{existingTriggerName};

    $toDelTriggers = $toDelTriggers."\n"."/* Drop trigger if exists $db\.aud_i_$table; /*";
    $Param->{toDelTriggers}=$toDelTriggers ;

    if( defined $useraut && $useraut ne ""){
        $userToWrite = $useraut;
    }
    elsif ( defined $usermon && $useraut eq "" ){
        $userToWrite = $usermon;
    }
    else
    {
        $userToWrite = $useraut;
    }

    if ( defined $usercheck && $usercheck ne "" && $useraut eq "" ){
    }
    else
    {
        $usercheck = '';
    }
    
    $cmd = "DELIMITER ##
    
    ";
    if(defined $acstatment &&  $acstatment ne "")
    {
        $cmd = $cmd."\t /* WARNING EXISTING TRIGGER FOUND\n";
        $cmd = $cmd."\t TRIGGER NAME = $db\.$existingTriggerName\n";
        $cmd = $cmd."\t GOING TO DROP IT AND INCLUDE IN THE NEW ONE*/\n";
        $cmd = $cmd."\t /* ADDING Trigger definition in in the Trigger Section*/ \n";
        
        $cmd = $cmd." Drop trigger if exists $db\.$existingTriggerName##
        ";
        $Param->{triggerbody} = $Param->{triggerbody}." \n /* SCHEMA = $db\n TABLE = $table\n TRIGGER_NAME= $existingTriggerName\n TRIGGER_ACTION=INSERT\n";
        $Param->{triggerbody} = $Param->{triggerbody}."TRIGGER_BODY= $acstatment\n";
        
    }
    $cmd = $cmd." Drop trigger if exists $db\.aud_i_$table##
            
            CREATE TRIGGER $db\.aud_i_$table
             AFTER INSERT ON $fullTablename FOR EACH ROW
             BEGIN
                    DECLARE sqlcode INT DEFAULT 0;
                    DECLARE CONTINUE HANDLER FOR 1054 SET sqlcode = 1054; 
                    DECLARE CONTINUE HANDLER FOR 1136 SET sqlcode = 1136;
             
                    SET \@username='';
                    SET \@action='INSERT';
                    SET \@host='';
                    SET \@connectionid=0;
                    SET \@appID='';
                    SET \@appIP='';
                    SET \@TODAY=0;
                    SET \@LASTINSERT=0;
                    
                    SELECT TO_DAYS(NOW())into \@TODAY;
                    
";
    if ($Param->{applicationUser} eq 0){
                    
    $cmd = $cmd."
                    SELECT SUBSTRING_INDEX(USER(),'\@',1) INTO \@username;
                    SELECT SUBSTRING_INDEX(USER(),'\@',-1) INTO \@host;
                    SELECT connection_id() into \@connectionid;
";
    }
    else{
    $cmd = $cmd."
                if (SELECT \@UserAppID IS NOT NULL) then
                    SELECT SUBSTRING_INDEX(USER(),'\@',1) INTO \@username;
                    SELECT SUBSTRING_INDEX(USER(),'\@',-1) INTO \@host;
                    SELECT \@UserAppID INTO \@appID;
                    SELECT \@UserAppIP INTO \@appIP;
                    SELECT connection_id() into \@connectionid;
                
                else              
                    SELECT SUBSTRING_INDEX(USER(),'\@',1) INTO \@username;
                    SELECT SUBSTRING_INDEX(USER(),'\@',-1) INTO \@host;
                    SELECT connection_id() into \@connectionid;            
                end if;\n
";
    }
    $cmd = $cmd."        
            
                    
                    if ($userToWrite) then
                    ";
                        $cmd = $cmd.printSYS_AUDIT_MAINLOG($db,$fullTablename,'A');
                    
                    $cmd = $cmd. "\n
                       SELECT LAST_INSERT_ID() INTO \@LASTINSERT;
                    end if;\n";


                           #this block insert data if a list od user to check exists;
                            if(defined $usercheck && $usercheck ne "")
                            {
                                $cmd = $cmd."\n -- check user audit \n";
                                $cmd = $cmd."
                    if ($usercheck) then
            
                    ";
                        $cmd = $cmd.printSYS_AUDIT_MAINLOG($db,$fullTablename,'A');
                    
                    $cmd = $cmd. "\n
                    end if;\n";

                            }

                            #This block reports the previously existing triggers in to the new one [START]
                            if(defined $acstatment &&  $acstatment ne "")
                            {
                                $cmd = $cmd."\n".$existingTriggerName.":   ".$acstatment." ".$existingTriggerName.";"
                            }
                            #This block reports the previously existing triggers in to the new one [END]

            if($keepHistory gt '0')
            {
                            $cmd = $cmd ."\n
                    if ($userToWrite) then

                            INSERT INTO $audDb.$table values($new\@action,LAST_INSERT_ID(),'NEW','A',\@TODAY,NULL);
                    end if;";
                            if(defined $usercheck && $usercheck ne "")
                            {
                                $cmd = $cmd."\n";
                                $cmd = $cmd."
                    if ($usercheck) then

                            INSERT INTO $audDb.$table values($new\@action,LAST_INSERT_ID(),'NEW','C',\@TODAY,NULL);
                    end if;
            
                            "};
            };

            
            $cmd = $cmd ."
            END##

            DELIMITER ;";
            
            
        if (defined($Param->{outfile})){
            
            print FILEOUT "\n", $cmd," \n";
            print FILEOUT "\n", "-- Insert Trigger for $table  [END]"," \n";
            
        }
            
            
    return \$Param;
}

sub printHistoryInsert($$$$)
{
    my $audDb = shift;
    my $table = shift;
    my $old = shift;
    my $typeLog = shift;
    my $returnedString = " INSERT INTO $audDb.$table values($old\@action,\@LASTINSERT,'OLD','$typeLog',\@TODAY,NULL);\n";
    
    return  $returnedString;   
}

sub printSYS_AUDIT_MAINLOG($$$)
{    
    my $db = shift;
    my $fullTablename = shift;
    my $typeLog = shift;
    my $returnedString = "            INSERT INTO security.SYS_AUDIT_MAINLOG (
                                                                            SYS_HISTOR_SCHEMA,
                                                                            SYS_HISTOR_TABLE,
                                                                            SYS_HISTOR_OPERATION_TYPE,
                                                                            SYS_HISTOR_DAY,
                                                                            SYS_HISTOR_USER,
                                                                            SYS_HISTOR_TERM,
                                                                            SYS_HISTOR_APPID,
                                                                            SYS_HISTOR_APPIP,
                                                                            SYS_HISTOR_SESSION_ID,
                                                                            SYS_HISTORY_AUDIT_TYPE
                                                                            ) VALUES(
                                       '$db',
                                       '$fullTablename',
                                       \@action, 
                                       \@TODAY,
                                       \@username,
                                       \@host,
                                       \@appID,
                                       \@appIP,
                                       \@connectionid,
                                       '$typeLog'
                                       );\n
                                       "
    
    
}

sub triggerBodyReplace($)
{
   my $Param = shift;
    
    my $db = $Param->{currentdb};
    my $table = $Param->{currentTable};
    my $useraut = $Param->{authorized};
    my $new = $Param->{currentNew};
    my $old = $Param->{currentOld};
    my $audDb = $Param->{audDb};

    my $cmd ="";
    my $dbh  = $Param->{dbh};
    my $acstatment;
    my $existingTriggerName ;
    my $methodToAudit = $Param->{methodToAudit};
    my $tablePrefix = "aud_";
    my $action_timing ="BEFORE";
    
    if($methodToAudit eq "DELETE")
    {
        $tablePrefix = $tablePrefix."d_";
    }
    elsif($methodToAudit eq "UPDATE")
    {
        $tablePrefix = $tablePrefix."u_";
    }
    elsif($methodToAudit eq "INSERT")
    {
        $tablePrefix = $tablePrefix."i_";
        $action_timing ="AFTER";
    }

    $cmd = "SELECT TRIGGER_NAME, ACTION_STATEMENT FROM INFORMATION_SCHEMA.TRIGGERS T where trigger_schema='$db' and event_object_schema='$db' and event_object_table='$table' and action_timing='$action_timing' and EVENT_MANIPULATION='$methodToAudit' and TRIGGER_NAME !='$tablePrefix$table'";
    

    my $sth = $dbh->prepare($cmd);
    $sth->execute();

    #my $recordSet = $sth->fetchrow_hashref();
    
    
    #if (defined($recordSet))
    #{
      my $row = undef;
      
      while ( $row = $sth->fetchrow_hashref() )
      {
        $acstatment = $row->{'ACTION_STATEMENT'};
        $existingTriggerName = $row->{'TRIGGER_NAME'};
        
            if(defined $acstatment)
            {
                print "========================================================\n";
                print "WARNING AN EXISTING TRIGGER IS FOUND IN TABLE $db.$table with name $existingTriggerName\n";

                #print $cmd."\n";
                print "========================================================\n";
                $Param->{acstatment} = $acstatment ;
                $Param->{existingTriggerName} = $existingTriggerName;
    
            }
            
      }
    if(!defined $acstatment )
    {
        $Param->{acstatment} = undef; ;
        $Param->{existingTriggerName} = undef;
    }
      
    #}
    $sth->finish();
    return \$Param;
}








sub checkAuditByTable($)
{
      my $Param = shift;  
      my $dbh  = $Param->{dbh};
      my %databases = %{$Param->{tables}};

    print "\n";
    print " --- === AUDIT check Section === --- [START]";
    print "\n";

    $Param->{authorizedListSQL} = getAuthorizedListSQL($Param);
    $Param->{monitoredListSQL} = getMonitoredListSQL($Param);
    $Param->{checkedListSQL} = getCheckedListSQL($Param);
  
    my $alltables = "";
    my %locReport;

    foreach my $key (sort keys %databases)
    {
        my @tables = @{$databases{$key}};
        my $dimTable = '';

        my $cmd = "use ".$key;
        my $sth = $dbh->do($cmd);
        my $time = localtime;
	
        my %columnHash ;
        my @columTypeDim ;
	my $tblDirty = 0;
	
        print "processing = $key [start] at $time \n";

        for (my $icounter = 0 ; $icounter <= $#tables; $icounter++)
        {
            my %columnHash ;
            

	    $dimTable = $tables[$icounter];
            
            #get table number of rows
            my $numberOfRows;
            my $audDb = $key."_audit";
            
            my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
            $mon = ++$mon;
            $year = $year+=1900;

            $cmd = "SELECT COLUMN_NAME, Data_TYPE, NUMERIC_PRECISION, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS where table_schema='$key' and table_name='$dimTable'";

	    $sth = $dbh->prepare($cmd);
	    $sth->execute();

	    #my $recordSet = $sth->fetchrow_hashref();
	    
	    
	    #if (defined($recordSet))
	    #{
	      my $row = undef;
              my $newColumns = "";  
              my $oldColumns = "";
              
	      while ( $row = $sth->fetchrow_hashref() )
	      {
                my @columTypeDim ;
		$newColumns = $newColumns."NEW\.".$row->{'COLUMN_NAME'}.",";
		$oldColumns = $oldColumns."OLD\.".$row->{'COLUMN_NAME'}.",";
                
                $columTypeDim[0] = $row->{'Data_TYPE'};
                $columTypeDim[1] = $row->{'NUMERIC_PRECISION'};
                $columTypeDim[2] = $row->{'CHARACTER_MAXIMUM_LENGTH'};
                $columnHash{$row->{'COLUMN_NAME'}} = \@columTypeDim
	      }
	    #}
	    $sth->finish();

            $Param->{currentDb} = $key;
            $Param->{currentTable} = $dimTable;
            
            $Param->{tbColumnStatus} = \%columnHash;
            
            $tblDirty=0;
            $tblDirty = getTableStatus($Param);
            #0 = stay as it is ; 1 do backup ; 2 generate audit table;
            
            if($tblDirty == 0)
            {
                print $dimTable." [NOT MODIFIED]\n";
            }
            elsif($tblDirty == 1)
            {
                print $dimTable." [MODIFIED Table will be BACKUP with name(".$audDb.".".$dimTable."_".$hour."_".$min."_"."_".$mday."_".$mon."_".$year.")]\n";
                print "\t ".$Param->{dirtyMessage};
            }
            elsif($tblDirty == 2)
            {
                print $dimTable." [NEW AUDIT on table ".$audDb." Table will be CREATED ]\n";
            }


            
            undef($Param->{tbColumns});
            
            $Param->{currentdb}=$key;
            $Param->{audDb}=$audDb;
            $Param->{currentTable}=$dimTable;
            $Param->{currentNew}=$newColumns;
            $Param->{currentOld}=$oldColumns;
        }

        $time = localtime;
        print "\nprocessing = $key [end] at $time ";
    }
    print "\n --- === AUDIT Check Section === --- [END]";

        
    return \$Param;
}




sub ShowOptions {
    print <<EOF;
How to use Audit
===========================================
Scope of audit is to generate a file that you can run in MySQL fin order to modify the target schema for auditing.
This is accomplish adding triggers for each audited table that will catch the not authorized modifications.
All the modification will be then saved in a parallel schema that will store all the modified data, in a structure
that is equal to the original one.

It is possible to run the audit package specifing all the parameters in command line format, or using a configuration file.

audit.pl --defaults-file=/temp/audit/test.ini

audit.pl -u=mysql_user -p=mysql -H=127.0.0.1 -P=5506 -a=root,app_user -i=employees,test -o=/test/audit/employees_13_01_2010.sql


To run the check, in order to identify inconsistency use the reportmodification parameter [0 it is not enabled | 1 it is enable]


--help, -h
    Display this help message

--defaults-file File containing all the settings in standard ini format (label = value)

--authorized|a User(s) authorized that will *NOT* be traked

--monitored List of people which WILL be audit (this works ONLY if authorized is blank)

--checked  List of people which WILL be audit with CHECK flag (this works ONLY if authorized is blank)

--host=HOSTNAME, -H=HOSTNAME    Connect to the MySQL server on the given host

--user=USERNAME, -u=USERNAME    The MySQL username to use when connecting to the server

--password=PASSWORD, -p=PASSWORD The password to use when connecting to the server

--port=PORT, -P=PORT  The socket file to use when connecting to the server

--includelist|i comma separated list of databases

--exludelist|x comma separeted list of databases

--outfile=FULLPATH, -o=FULLPATH full path (including file name), containing the whole set of modification to be applied.

--reportmodification perform silent consistency check

--keephistory = 0 (disabled) | 1 (enabled) this will allow the trigger to store the modified values in history tables

--reportmodification = 0 (default) | 1 (enable) Enabling it will set the procedure in report modifaction ONLY mode.
    A report of modified (if any) table(s) will be printed in the standard out, no action will be performed.

--silent = 0 (default) | 1 disable the confirmation windows when running (be really sure you type correctly what you need/want)

The user which connect to the db to run audit, need to have access to all the db to audit and all the audit database created

EOF
}
################################
#INVOCATION SECTION     [START]#
################################

 my %databaseTablesMap;
 my %reports;
 my $dbh = get_connection($dsn, $user, $pass);
 $Param->{dbh}=$dbh;

 if ( defined $Param->{reportmodification} && $Param->{reportmodification} eq '0')
 {
    $Param = createAuditSysTable($Param);
 }
 $Param = getDataBases($Param);
 $Param = getTables($Param);
 

 if ( defined $Param->{reportmodification} && $Param->{reportmodification} eq '0')
 {
    createAuditByTable($Param);
 }
 elsif( defined $Param->{reportmodification} && $Param->{reportmodification} eq '1')
 {
    checkAuditByTable($Param);
 }



################################
#INVOCATION SECTION     [ENDS]#
################################

$Param->{dbh}->disconnect();
if( defined $Param->{outfile}){
    close FILEOUT;
}

exit(0);

#SET SQL_MODE=PIPES_AS_CONCAT;
#INSERT INTO security.debug VALUES("DEBUG");
#INSERT INTO security.debug VALUES(NEW.autoInc);
#INSERT INTO security.debug VALUES(NEW.a);
#INSERT INTO security.debug VALUES(NEW.uuid);
#INSERT INTO security.debug VALUES(NEW.b);
#INSERT INTO security.debug VALUES(NEW.c);
#INSERT INTO security.debug VALUES(NEW.counter);
#INSERT INTO security.debug VALUES(NEW.time);
#INSERT INTO security.debug VALUES(NEW.partitionid);
#INSERT INTO security.debug VALUES(NEW.strrecordtype);
#INSERT INTO security.debug VALUES(@action);
#INSERT INTO security.debug VALUES(LAST_INSERT_ID());
#INSERT INTO security.debug VALUES('OLD');
#INSERT INTO security.debug VALUES('A');
#INSERT INTO security.debug VALUES(@TODAY);            