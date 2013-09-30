#!/usr/bin/perl
#######################################
#
# Mysql table audit v 1.0.1 (2010) 
#
# Author Marco Tusa 
# Copyright (C) 2001-2003, 2008
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

######################################
package commonfunctions;
use strict;
use visualization;
use ConfigIniSimple;

use DirHandle;

use vars qw(@ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $VERSION);

use Exporter;
$VERSION = 1.00;              # Or higher
@ISA = qw(Exporter);
@EXPORT      = qw(
    URLDecode
    URLEncode
    debugEnv
    writeNull
    getConfirmation
    getPasswordConfirmation
    getExcludeList
    getIncludeList
    checkForLists
    getArrayAsComma
    loadIniObject
    writeIniObject
    loadSettingsFromIni
    saveSettings
    removeAllFile
    checkDirectoryAndCreate
    big_sub
    make_bigint
    big_multiply
    big_add
    increment
    array_sum
    );       # S

sub loadSettingsFromIni($$)
{
    my $conf = shift;
    my $Param = shift;
    my $appHeader = $Param->{appheader};
    my $string .=$appHeader;
    
    $string .="\t=============================================================================\n";
    $string .="\t\t\t\t Choose one of the stored configuration\n";
    $string .="\t=============================================================================\n";
    my $newconfNumber = (keys(%{$conf}));
    
    my $key;
    my $pCounter = 1;
    my @auds;
    foreach $key (keys %{$conf})
    {
        
        if(substr($key,0,2) ne '__')
        {
            $auds[$pCounter] = $key;
            $string .="\t[".$pCounter++."] ".$key."\n";
        }
        
    }

    $string .="\n\t[N] New entry\n";    
    $string .="\t[0] EXIT\n";    
    $string .="\t=============================================================================\n";
   
    if( defined $Param->{reportmodification}  && $Param->{reportmodification} eq '1')
    {
	my $keyHash = $conf->{$auds[1]};
	return $keyHash;
    }
   
    print $string;
    my $listchoice = promptUser("","1","0|1(default)|..");

    
    if($listchoice eq '0' )
    {
        exit(0);        
    }
    elsif($listchoice eq 'N' )
    {
        return  undef; 
    }
    
    my $keyHash = $conf->{$auds[$listchoice]};

    return $keyHash;
    
}

sub saveSettings($$$)
{
    my $settings = shift;
    my $settingsPath = shift;
    my $settingsName = shift;
    my $conf;
    
    if(!defined($conf))
    {
        $conf = loadIniObject($settingsPath) ;       
    }
    $conf->{$settingsName}=$settings;
    writeIniObject($conf);
    print "file saved in ".$conf->{__file__};
    return 1;
}

sub URLDecode($) {
    my $theURL = shift;
    $theURL =~ tr/+/ /;
    $theURL =~ s/%([a-fA-F0-9]{2,2})/chr(hex($1))/eg;
    $theURL =~ s/<!--(.|\n)*-->//g;
    return $theURL;
}

sub loadIniObject($)
{
    my $conf = new ConfigIniSimple;
    my $file = shift;
  #
  ## Read the config file.
  #$conf->{mypath} = $file;
  $conf->read($file);
  return $conf
  #$conf->read("/mnt/d/work/mysql/programs/mysqltools/audit/settings.ini");
  #
  
  #my $newAudithas = {};
  #
  #$newAudithas->{port}='3320';
  #$newAudithas->{host}='1270.0.1';
  #
  #$conf->{newaudit} = $newAudithas;
  #
  ### Change the port from "Server02" block
  #$conf->{Server02}->{port} = 2236;
  ##
  ##$conf->{}->{port} = 2236;
  ### Change the "server" to "Server02"
  #$conf->{default}->{server} = 'Server02';
  ##

    
}
sub writeIniObject($)
{
  my $conf = shift;  
  ## Write the changes.
  $conf->write ($conf->{mypath});
  #
    
}

sub URLEncode($) {
    my $theURL = shift;
   $theURL =~ s/([\W])/"%" . uc(sprintf("%2.2x",ord($1)))/eg;
   return $theURL;
}

sub debugEnv{
    my $key = keys %ENV;
    foreach $key (sort(keys %ENV)) {

       print $key, '=', $ENV{$key}, "\n";

    }
}

sub writeNull($){
    my $retValue = shift;
    if(!defined($retValue))
    {
	return "NULL";
    }
    elsif(substr($retValue, 0, 4) eq "ENUM")
    {
	return "ENUM()";
    }
    else{
	return $retValue;
    }
   
sub getConfirmation($)
    {
        
        my $value = shift;
        my $confirm = promptUser("Do you confirm : $value","y", "Y/n");
        return $confirm;
        
    }

sub getPasswordConfirmation($)
    {
        
        my $value = shift;
        my $confirm = promptUser("Please retype password","", "");
        if($confirm eq $value)
        {
            return 'y';
        }
        return 'n';
        
    }

}

sub getExcludeList($)
{
    my $Param = shift;
    my @excludelist;
    my $invalidator = $Param->{invalidator};
    
    if( (defined $Param->{excludelist}) and ($Param->{excludelist} ne "")){
       @excludelist = split(/,/,join(',',$Param->{excludelist}));
       $invalidator = $invalidator + 1;
       $Param->{invalidator} = $invalidator;
       $Param->{excludelist}=\@excludelist;
    }
    else
    {
        undef $Param->{excludelist};    
    }
    
    return $Param;    
}


sub getIncludeList($)
{
    my $Param = shift;
    my @includelist;
    my $invalidator = $Param->{invalidator};

    if((defined $Param->{includelist}) and ($Param->{includelist} ne "")){
       @includelist = split(/,/,join(',',$Param->{includelist}));
       $invalidator = $invalidator + 1;
       $Param->{invalidator} = $invalidator;
       $Param->{includelist}=\@includelist;
    }
    else
    {
        undef $Param->{includelist};
    }
    
    return $Param;    
   
}

sub checkForLists($)
{
    my $Param = shift;
    my $appHeader = $Param->{appheader};
    my $invalidator = $Param->{invalidator};
    my $string .=$appHeader;
    #my $string ="";    

    if ($invalidator > 1 ){
        
        print "Error: it is not Possible to define both Exclude AND Include list \nPlease check your parameter definition.";
        exit(15);
    }
    elsif($invalidator == 0)
    {
        #print $appHeader."\nYou must define at leas a value for Exclude *OR* Include list
        #=============================================================================
        #    Choose:
        #    [1] Exclude list for inserting one or more values (comma separated)
        #    [2] Include list for inserting one or more values (comma separated)
        #
        #    [0] EXIT
        #=============================================================================
        #";
        $string .="\n\tYou must define at leas a value for Exclude *OR* Include list\n";
        $string .="\t=============================================================================\n";
        $string .="\tChoose:\n";
        $string .="\t[1] Exclude list for inserting one or more values (comma separated)\n";
        $string .="\t[2] Include list for inserting one or more values (comma separated)\n\n";
        $string .="\t[0] EXIT\n";
        $string .="\t=============================================================================\n";

        print $string;
        my $listchoice = promptUser("","2","0|1|2(default)");
        print "\n";

        
        if($listchoice == 0)
        {
            exit(0);            
        }
        elsif($listchoice == 1)
        {
            my $showText = !defined($Param->{excludelist})? "none": $Param->{excludelist};
            my $default  = !defined($Param->{excludelist})? '<none>': $Param->{excludelist};
            my $list = '';

            $list = promptUser("list values",$default,$showText);
            $Param->{excludelist} = $list;
            $Param = getExcludeList($Param);
            $Param->{invalidator} = 1;
            undef($Param->{includelist});

        }
        elsif($listchoice == 2)
        {
            my $showText = !defined($Param->{includelist})? "none": $Param->{includelist};
            my $default  = !defined($Param->{includelist})? '<none>': $Param->{includelist};
            my $list = '';
            $list = promptUser("list values",$default,$showText);
            $Param->{includelist} = $list;
            $Param = getIncludeList($Param);
            $Param->{invalidator} = 1;
            undef($Param->{excludelist});
           
        }
        else
        {
           $Param->{invalidator} = 0; 
        }

    }
    return $Param;
    
}
sub getArrayAsComma($)
{
    my @ar = @{(shift)};
    my $boundary = $#ar;
    my $commaList = '';
    
    
    for(my $counter = 0 ; $counter <= $boundary; $counter++)
    {
        if($counter == 0)
        {
            $commaList = $ar[$counter];
        }
        else
        {
            $commaList = $commaList.",".$ar[$counter];

        }
    }
    
    return $commaList;
    
}

sub getReport($$$)
{
    my $parameters = shift;
    my $application = shift;
    my $HeadText = shift;
    
    
}

sub removeAllFile($$)
{
    my $dir = shift;
    my $debug = shift;
    
    if($debug == 1)
    {
        print "Going to delete files in $dir :\n";
    }
    
    my $d = new DirHandle $dir;
    if(!defined($d))
    {
        return 1;
    }

    #opendir(BIN, $dir);# or die "Can't open $dir: $!";
    while( defined (my $file = $d->read) ) {
    if($debug == 1)
    {
        print "$file\n" if -T "$dir/$file";
    }
        my $delete = $dir."/".$file;
        unlink($delete);
        
    }
    if(defined(my $d))
    {
        $d->close;
        return rmdir($dir);
    }
    
    
}

sub checkDirectoryAndCreate($) {
   my $dir = shift;
   my $dh = DirHandle->new($dir);
   
   if(!defined($dh))
   {
        print "can't opendir $dir: I am going to try to create it; \n";
        mkdir $dir;
        chmod 0777, $dir;
        $dh = DirHandle->new($dir);
        return 1;
        #die();
   }
   else
   {
    return 1;
   }
   
   if(!defined($dh))
   {
        return 0;
   }
   
   
   #return sort                     # sort pathnames
   #       grep {    -f     }       # choose only "plain" files
   #       map  { "$dir/$_" }       # create full paths
   #       grep {  !/^\./   }       # filter out dot files
   #       $dh->read();             # read all entries
}


# ============================================================================
# Subtract two big integers as accurately as possible with reasonable effort.
# This is tested in t/mysql_stats.php and copied, without tests, to
# ss_get_by_ssh.php.  $force is for testability.
# ============================================================================
sub big_sub ($$) {
    
    my $left = shift;
    my $right = shift;
   # my $force = shift;
    
   
   if ( !defined $left  ) { $left = 0; }
   if ( !defined $right ) { $right = 0; }
    my $x = Math::BigInt->new($left);
    my $xint = $x->bsub($right);
    return $xint->{value}[0];
}

# ============================================================================
# Returns a bigint from two ulint or a single hex number.  This is tested in
# t/mysql_stats.php and copied, without tests, to ss_get_by_ssh.php.
# ============================================================================
sub make_bigint ($$) {
   my $hi = shift;
   my $lo = shift;   
   
   if (defined $lo ) {
      # Assume it is a hex string representation.
      my $x = Math::BigInt->new($hi);
      my $xint = $x->as_number;
      return $xint->{value}[0];
   }
   else {
      $hi = $hi ? $hi : '0'; # Handle empty-string or whatnot
      $lo = $lo ? $lo : '0';
      return big_add(big_multiply($hi, 4294967296), $lo);
   }
}

# ============================================================================
# Multiply two big integers together as accurately as possible with reasonable
# effort.  This is tested in t/mysql_stats.php and copied, without tests, to
# ss_get_by_ssh.php.  $force is for testability.
# ============================================================================
sub big_multiply ($$$) {
   my $left = shift;
   my $right = shift;
   my $force = shift;
   
   my $x = Math::BigInt->new($left);
   my $bx = $x->bmul($right);
   return $bx->{value}[0];
  
 
}
# ============================================================================
# Add two big integers together as accurately as possible with reasonable
# effort.  This is tested in t/mysql_stats.php and copied, without tests, to
# ss_get_by_ssh.php.  $force is for testability.
# ============================================================================
sub big_add ($$$) {
    my $left = shift;
    my $right = shift;
    my $force = shift;
    
   if ( !defined $left )
      {
	 $left = 0;
    }
   if ( !defined $right) { $right = 0; }
  
    my $x = Math::BigInt->new($left);
    my $bx = $x->badd($right);
     return $bx->{value}[0];
 
   
 
}


# ============================================================================
# Safely increments a value that might be null.
# ============================================================================
sub increment(\%$$) {
   my $hash = shift;
   my $key = shift;
   my $howmuch = shift;
    
   if ( defined $hash->{$key}) {
      if ($hash->{$key} > 0)
      {
	my $value = int($hash->{$key});
	$hash->{$key} = ($value + $howmuch);
      }
      else
      {
	$hash->{$key} = ($howmuch);
      }
   }
   else
   {
	$hash->{$key} = $howmuch;

   }
   return $hash;
}

# ============================================================================
# Sum the content of the array assuming it is numeric 
# ============================================================================
sub array_sum(@){
    my @array = shift;
    my $acc = 0;
    foreach (@array){
      $acc += $_;
    }
    return $acc;
}
