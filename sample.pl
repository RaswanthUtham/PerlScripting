#! /usr/bin/perl

format EMPLOYEE =

@<<<<<<<<< @< @<<<<<<<<< @<<     @<<<<< 
$name, $age, $date, $opt, $salary
.

format EMPLOYEE_TOP =
=======================================
Name      Age  Date      Opt     Hii 
=======================================
.

select(STDOUT);

$~ = EMPLOYEE;
$^ = EMPLOYEE_TOP;
open(FR, "<input.txt") or die "could not open the  file or $!";
readMe(FR);

while(<FR>)
{
    print "$_";
}

open(WR, ">output.txt") or die "could not open the file or $!";

while(<FR>)
{

}


close FR;