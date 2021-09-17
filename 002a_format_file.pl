#!/usr/bin/perl

print "Content-type: text/html\n\n";


@First_Name = qw(Karthik Raman Shivam Tarun);
@Last_Name = qw(Singh Biswas Anand Mishra);
@ID = qw(101 186 009 359);
@Phone_Number = qw(963x 885x 734x 921x);


format DATA =
@<<<<<<<<<<<<<<<@<<<<<<<<<<<<<<<@<<<<<<<<<<<<<<<@<<<<<<<<<<<<<<
$First_Name[$k], $Last_Name[$k], $ID[$k],		 $Phone_Number[$k]

.

open(FILE, ">file.txt");
select FILE;
$~ = DATA;

$k = 0;
while ($k <= $#First_Name)
{
	write;
	$k++;
}

close FILE;
