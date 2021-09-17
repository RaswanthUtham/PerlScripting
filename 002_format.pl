
sub process_email_report($$$)
{
    format EMPLOYEE =

@<<<<<<<<< @< @<<<<<<<<< @<<     @<<<<< 
$name, $age, $date, $opt, $salary
.

format EMPLOYEE_TOP =
=======================================
Name      Age  Date      Opt     Hii 
=======================================
.

open(FW, ">file.txt") or die;

select FW;
$~ = EMPLOYEE;
$^ = EMPLOYEE_TOP;


open(FR, "<sample.txt") or die "Couldn't open file file.txt, $!";
# "ras 27 2018-09-89 buy high"
while(<FR>) {
    @x = split(" ", $_);
    $name = $x[0];
    $age = $x[1];
    $date = $x[2];
    $opt = $x[3];
    $salary = $x[4];
    write;
}

close FW;
close FILE;
}

process_email_report();