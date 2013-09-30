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

#######################################
package visualization;

use strict;
use vars qw(@ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $VERSION);

use Exporter;
$VERSION = 1.00;              # Or higher
@ISA = qw(Exporter);
@EXPORT      = qw(promptUser);       # Symbols to autoexport (:DEFAULT tag)
#@EXPORT_OK   = qw(...);       # Symbols to export on request
#%EXPORT_TAGS = (TAG1 => [promptUser]);# Define names for sets of symbols


sub promptUser ($$$){
 my ($promptString,$defaultValue,$defaultValueText);
   #-------------------------------------------------------------------#
   #  two possible input arguments - $promptString, and $defaultValue  #
   #  make the input arguments local variables.                        #
   #-------------------------------------------------------------------#

  $promptString = shift;
  $defaultValue = shift;
  $defaultValueText=shift;

   #-------------------------------------------------------------------#
   #  if there is a default value, use the first print statement; if   #
   #  no default is provided, print the second string.                 #
   #-------------------------------------------------------------------#

   if ($defaultValueText) {
      print $promptString, "[", $defaultValueText, "]: ";
   } else {
      print $promptString, ": ";
   }
   
   
   $| = 1;               # force a flush after our print
   $_ = <STDIN>;         # get the input from STDIN (presumably the keyboard)
   

   #------------------------------------------------------------------#
   # remove the newline character from the end of the input the user  #
   # gave us.                                                         #
   #------------------------------------------------------------------#

   chomp;

   #-----------------------------------------------------------------#
   #  if we had a $default value, and the user gave us input, then   #
   #  return the input; if we had a default, and they gave us no     #
   #  no input, return the $defaultValue.                            #
   #                                                                 # 
   #  if we did not have a default value, then just return whatever  #
   #  the user gave us.  if they just hit the <enter> key,           #
   #  the calling routine will have to deal with that.               #
   #-----------------------------------------------------------------#

   if ("$defaultValue") {
      if(defined($_) && $_ ne '' )
      {
         return $_ ;    # return $_ if it has a value
      }
      else
      {
         return $defaultValue;
      }
   } else {
      return $_;
   }
}

