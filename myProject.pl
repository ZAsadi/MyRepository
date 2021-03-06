use  feature "switch";
use DBI;
$dbh_ref=DBI->connect("dbi:Pg:database=postgres",'postgres','root',{AutoCommit=>1})or die "connection";


print "Wellcome!!! Our application supports four different categories of annotate relations\n";
print"1.Multiset\n";
print"2.Uncertain\n";
print"3.Provenance\n";
print"4.Probabilistic\n";
print"Please enter a digit corresponding to the relations of your interest:\n";


#list of the subroutines
sub parser($$$);
sub queryEvaluation($$$$$);
sub schema_join_finder($$);
sub table_generator ($$$);
sub initial_insertion($$);
sub drop_table($);
sub update_annotation_join($$$);
sub schema_finder($$);
sub schema_finder_union_selection($) ;
sub schema_finder_groupby_union($); 
sub update_annotation_union($$$);
sub update_annotation_projection($$$$);

chomp(my $type =<>);
print"We are ready to evaluate your query.Please enter your query:\n";
chomp(my $query=<>);
my @query_split=split(' ',$query);
my $len=scalar(@query_split);
my $counter=0;
@operandsOfQuery=();
@opeartorsOfQueryS=();
for( $i=0;$i<$len;$i++){
print"$query_split[$i]\n";
}

my @operators=("j","u","s","p");

my @operatorsWithCond=("j","s","p");
#print $operatorsWithCond[1];
my $len_op=scalar(@operatorsWithCond);
#in the following for block we get to know how many operators in the query need conditions.
for($i=0;$i<$len;$i++){
	for ($j=0;$j<$len_op;$j++){
		if ($query_split[$i] eq $operatorsWithCond[$j]){
			$counter++;
		}
	}
}
print $counter;

print"Please enter the corresponding conditions based on Bracket Off from left to right.\n";
@conditions=();
for ($i=0;$i< $counter;$i++){
	chomp(my $input=<>);
	push(@conditions,$input);
}

for ($i=0;$i<scalar(@conditions);$i++){
	print"$conditions[$i]\n";
	
}
#exit 1;
print"print the content of the array by use of for loop!!\n";

print"@conditions\n";

parser($query,$type,\@conditions);
my $precedence=1;
sub parser($$$)
{
	my ($querylocal,$typelocal,$conditionsaddress)=@_;
	my @conditionsArray=@{$conditionsaddress};

	print"$querylocal,$typelocal,@conditionsArray\n";
	my @querylocalArray=split(' ',$querylocal);

        my $len=scalar(@querylocalArray);
        my $j=0;
        my $flag=0;
	while ($j<$len){
		if (($querylocalArray[$j] ne "(") && ($querylocalArray[$j] ne ")" )){
                        for($k=0;$k<4;$k++) {
                        	if ($querylocalArray[$j]  eq $operators[$k] ) {
                        		push (@operatorsOfQuery,$querylocalArray[$j]);
                        		$flag=1; }
                        }	                    
                     
			if($flag!=1) {         	 
                               push (@operandsOfQuery,$querylocalArray[$j]); }
                 }           
                 else{
                 	if ($querylocalArray[$j] eq ")" ){
                 		my $operatorPoped=pop(@operatorsOfQuery);
                 		print "\n\n\n";
                 		print"$operatorPope\n";
                 		
                 		if ($operatorPoped eq "j" || $operatorPoped eq "u"){
                 			my $firstOperand=pop(@operandsOfQuery);
                 			my $secondOperand=pop(@operandsOfQuery);
                 			
                                        if ($operatorPoped eq "j"){
                                        push(@operandsOfQuery,queryEvaluation($type,$firstOperand,$secondOperand,$operatorPoped,$conditions[$precedence]));
                                        
                                        $precedence++;
                                        }
                                        else{
          -                              push(@operandsOfQuery,queryEvaluation($type,$firstOperand,$secondOperand,$operatorPoped,null));
                                        
                                        }	
                                 
                                 }
                                 else{
                                 	
                                 	my $unaryOperand=pop(@operandsOfQuery);
                                 	push(@operandsOfQuery,queryEvaluation($type,$unaryOperand,null,$operatorPoped,$conditions[$precedence]));
                                        $precedence++;
                                     }
      
                        }
                 
                  }   
              $j++; 
              $flag=0; 
               
       }          
                       
       print "@operatorsOfQuery\n";
       print "@operandsOfQuery\n";
       
  } 
 sub queryEvaluation($$$$$){
 	
 	print"inside the subroutine!";
 	my($type,$firstOperand,$secondOperand,$operatorPoped,$condition)=@_; 
 	print"$type,$firstOperand,$secondOperand,$operatorPoped,$condition\n";
 	my $operand=$operatorPoped;
 	schema_join_finder($firstOperand,$secondOperand);
 	print "$operand\n";
        print "$queryString\n";
       
 	
        given( $operand ) {
        	  when ( "j"){  
        	  my $joinStr="select * from $secondOperand join $firstOperand on $condition";
        	  print "$joinStr\n";
        	  my $joinQry=$dbh_ref->prepare($joinStr);
        	  $joinQry->execute();
        	  print "$joinStr\n";
        	  # my $joinschema=schema_join_finder($secondOperand,$firstOperand);
        	  my $tableName=table_generator (schema_join_finder($secondOperand,$firstOperand),"temp","join");
    
        	  initial_insertion($tableName,$joinStr);
        	  
        	  update_annotation_join($secondOperand,$firstOperand,$type);
        	  }
        	     
                  when ( "u"){
                  my $unionStr="(select * from $firstOperand) union (select * from $secondOperand)";
                  my $unionQry=$dbh_ref->prepare($unionStr);
        	  $unionQry->execute();
        	  my $tableName=table_generator (schema_finder_union_selection($secondOperand),"temp","union");
        	  initial_insertion("temp_union",$unionStr);
        	  update_annotation_union($secondOperand,$firstOperand,$type);
        	  
                  }
                 
                 
                  my $annotation=();
                  my $annotationType=();
                  when ("p" ){
                  print $condition;
                  given( $type){
        	          when (1){
        	          $annotation="multiplicity";
        	          $annotationType="int";
        		  print"inside switch\n";} 	
                  }
                  my $modifiedCondition=$condition.",".$annotation;
                  my $projectionStr= "select $modifiedCondition from $firstOperand";
                  print"\n\n\n";
                  print $projectionStr;
                  #exit 1;
                  my $projectionQry=$dbh_ref->prepare($projectionStr);
        	  $projectionQry->execute();
        	  my $projectionSchema=schema_finder($firstOperand,$condition);
        	  $projectionSchema=$projectionSchema.", ".$annotation." ".$annotationType;
        	  print $projectionSchema;
                  my $tableName=table_generator ($projectionSchema,"temp","projection");
        	  initial_insertion("temp_projection",$projectionStr);
        	  update_annotation_projection($firstOperand,$projectionSchema,$condition,$type);
                  }
                  
                  when ("s" ){
                  my $selectStr= "select * from $firstOperand where $condition";
                  print "$selectStr\n";
                  my $selectQry=$dbh_ref->prepare($selectStr);
        	  $selectQry->execute();
        	  my $tableName=table_generator (schema_finder_union_selection($firstOperand),"$firstOperand","s");
        	  initial_insertion($tableName,$selectStr);
        	 
        	  
        }            
         
    }  
 } 
   
##This subroutine finds the schema of the result of join
sub schema_join_finder($$) {

		my ($table1,$table2) = @_;
		my $table=$table1;
		my $no=1;
		my $schema=();
		while($no<3){
		#my @con_arr=split(',',$cond);
		my $sth_column_info = $dbh_ref->column_info( '', '%', $table , undef );
	
		my $aoa_ref = $sth_column_info->fetchall_arrayref; # <- chg. to arrayref, no parms

		for my $aref (@$aoa_ref) {
		my($name,$type)=0;
		my @list = map $_ // 'undef', @$aref;
		$name=$list[3];
        
		$type=$list[5];
        
                    if($schema eq undef) {
                        $schema=$table."_".$name." ".$type;
                    }
                    else {
                        $schema=$schema.",".$table."_".$name." ".$type;
                    }
               
        }
        $no++; 
        $table=$table2;     
        }
        print " $schema,inside schema_join_finder";
	return $schema ;
	} 
	
#This subroutine finds the schema of the result of union and selection	
 sub schema_finder_union_selection($) 
 {
	my ($table) = @_;
	my $schema=();
	#my @con_arr=split(',',$cond);
	my $sth_column_info = $dbh_ref->column_info( '', '%', $table , undef );
	my $aoa_ref = $sth_column_info->fetchall_arrayref; # <- chg. to arrayref, no parms

	for my $aref (@$aoa_ref) {
		my($name,$type)=0;
		my @list = map $_ // 'undef', @$aref;
		$name=$list[3];
		$type=$list[5];
           
		if($schema eq undef) {
                        $schema=$name." ".$type;
                    }
               else {
                        $schema=$schema.",".$name." ".$type;
                    }
         }
        print $schema;
	return $schema;	
}	


 sub schema_finder_groupby_union($) 
 {
	my ($table) = @_;
	my $schema=();
	#my @con_arr=split(',',$cond);
	my $sth_column_info = $dbh_ref->column_info( '', '%', $table , undef );
	my $aoa_ref = $sth_column_info->fetchall_arrayref; # <- chg. to arrayref, no parms

	for my $aref (@$aoa_ref) {
		my($name,$type)=0;
		my @list = map $_ // 'undef', @$aref;
		$name=$list[3];
		$type=$list[5];
           
		if($schema eq undef) {
		        if ($name ne "multiplicity"){
                        $schema=$name;
                        }
                 }
               else {
                        if ($name ne "multiplicity"){
                        $schema=$schema.",".$name;
                        }
                     }   
         }
        print $schema;
	return $schema;	
}


sub schema_finder($$) {

    my ($table,$cond) = @_;
    my $schema=();
    my @con_arr=split(',',$cond);
    my $sth_column_info = $dbh_ref->column_info( '', '%', $table , undef );
    my $aoa_ref = $sth_column_info->fetchall_arrayref; # <- chg. to arrayref, no parms

    for my $aref (@$aoa_ref) {
        my($name,$type)=0;
        my @list = map $_ // 'undef', @$aref;
        $name=$list[3];
        $type=$list[5];
            for(my $j=0;$j<scalar(@con_arr);$j++) {
                if($name eq $con_arr[$j]) {
                    if($schema eq undef) {
                        $schema=$name." ".$type;
                    }
                    else {
                        $schema=$schema.",".$name." ".$type;
                    }
                }
            }
    }

    return $schema;

}
 
sub table_generator ($$$)
 {
       my ($schema,$firstOperand,$secondOperand) = @_;
       my $tableName=$firstOperand."_".$secondOperand;
       drop_table($tableName);
       my $createStr=" create table $tableName($schema)"; 
       print  $createStr;
       my $createQry=$dbh_ref->prepare($createStr);
       $createQry->execute();
       print"Inside Table_generator";
       return($tableName);
       
 }
 
 sub drop_table($)
 {	     
	my ($tableName)=@_;
	my $dropStr="Drop table $tableName ";
	my $dropQry=$dbh_ref->prepare($dropStr);
	$dropQry->execute();
	print"Inside Drop Table";
 }   
 
     
sub initial_insertion($$)
{
	my ($tableName,$queryString) = @_;
	my $insertStr="insert into $tableName ($queryString)";
	print $insertStr;
	my $insertQry=$dbh_ref->prepare($insertStr);
        $insertQry->execute();
        print"Inside initial_insertion";
	return($tableName);
	
}
sub update_annotation_join($$$)
 {
        my ($firstOperand,$secondOperand,$type) = @_;	
        my $tableName=$firstOperand."_".$secondOperand;
        my $annotation=();
        given( $type){
        	when (1){
        	        $annotation="multiplicity";
        		print"inside switch\n";
        		
        	}
        	
        }
        my $firstAnno=$firstOperand."_".$annotation;
        my $secondAnno=$secondOperand."_".$annotation;
        my $updateStr="update temp_join set $firstAnno=$firstAnno*$secondAnno";
        my $updateQry=$dbh_ref->prepare($updateStr);
        $updateQry->execute();
        print"Inside update_annotation_join!\n";
        my $dropClmStr="alter table temp_join drop column $secondAnno ";
        my $dropClmSQry=$dbh_ref->prepare($dropClmStr);
        $dropClmSQry->execute();
        print"Inside update_annotation_join! DROP COLUMN\n";
        my $renameClmStr="alter table temp_join rename column $firstAnno to $annotation";
        my $renameClmSQry=$dbh_ref->prepare($renameClmStr);
        $renameClmSQry->execute();
        
        print"Inside update_annotation_join!RENAME COLUMN\n";
        
        my $groupbySchema=schema_finder_groupby_union("temp_join");
        my $tableName=table_generator (schema_finder_union_selection("temp_join"),$firstOperand,$secondOperand);
        my $updateStr=" insert into $tableName ($groupbySchema,multiplicity) (select $groupbySchema,sum ($annotation) from temp_join group by $groupbySchema)";
        print $updateStr;
        my $updateQry=$dbh_ref->prepare($updateStr);
        $updateQry->execute();
        print"Inside update_annotation_union!\n";
        return($tableName);
     
   }     
        	  
sub update_annotation_union($$$)
{    
        my ($firstOperand,$secondOperand,$type) = @_;
        my $tableName=$firstOperand."_".$secondOperand;
        my $annotation=();
        given( $type){
        	when (1){
        	        $annotation="multiplicity";
        		print"inside switch\n";} 	
        }
        my $groupbySchema=schema_finder_groupby_union($firstOperand);
        my $tableName=table_generator (schema_finder_union_selection($firstOperand),$firstOperand,$secondOperand);
        my $updateStr=" insert into $tableName (select $groupbySchema,sum ($annotation) from temp_union group by $groupbySchema)";
        print $updateStr;
        my $updateQry=$dbh_ref->prepare($updateStr);
        $updateQry->execute();
        print"Inside update_annotation_union!\n";
        return($tableName);
}

sub update_annotation_projection($$$$)
{    
        my ($firstOperand,$schemaProjection,$condition,$type) = @_;
        my $tableName=$firstOperand."_p";
        my $annotation=();
        given( $type){
        	when (1){
        	        $annotation="multiplicity";
        		print"inside switch\n";} 	
        }
        my $tableName=table_generator ($schemaProjection,$firstOperand,"p");
        my $updateStr=" insert into $tableName (select $condition,sum ($annotation) from temp_projection group by $condition)";
        print $updateStr;
        my $updateQry=$dbh_ref->prepare($updateStr);
        $updateQry->execute();
        print"Inside update_annotation_union!\n";
        return($tableName);
}	        	  