use  feature "switch";
use DBI;
$dbh_ref=DBI->connect("dbi:Pg:database=postgres",'postgres','root',{AutoCommit=>1})or die "connection not working";


print "Wellcome!!! Our application supports four different categories of annotate relations\n";
print"1.Multiset\n";
print"2.Provenance\n";
print"3.Uncertain\n";
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
sub cond_modification($);
sub get_question_mark($);
sub annotation_reducer($);
sub annotation_reducer_join($);
sub operand_finder($);
chomp(my $type =<>);
print"We are ready to evaluate your query.Please enter your query:\n";
chomp(my $query=<>);
my @query_split=split(' ',$query);
# given probabilistic relation we need to add a new column called ev which stands for event variable to the table. 
#in the following piece of code we alter tables by adding a new column and then we randomly generate one event variable
# which consists of the table name and one integer between one and 100.
given ($type) {
	when(4){
		my $listOfOperandAddress=operand_finder($query);   # we are getting the address of the hash table containing the names of the tables in the query.
		#print @listOfOperandAddress;
		my %listOfOperand=();
		%listOfOperand=%{$listOfOperandAddress};
		# while(my ($k,$v)=each (%listOfOperand))
		# {
			# print $k;
		# }
		# exit 0;
		
		my $length=0;
		my @hashToArray='';
		while(my ($k,$v)=each (%listOfOperand)){
			$hashToArray[$length]=$k;
			$length++
		}
		for (my $i=0;$i<scalar(@hashToArray);$i++){
			print $hashToArray[$i];
		}
		
		#exit 0;
	        my $i=0;
		for ( $i=0;$i<scalar(@hashToArray);$i++){
			print "\n$hashToArray[$i]\t $i\n";
			my $schema=schema_finder_union_selection($hashToArray[$i]);
			print $schema;
		        #exit 0;
			@schemaArray=split(',|\s',$schema);
			my $type= $schemaArray[(scalar(@schemaArray))-1];
			print $type;
			#exit 0;
			
			
			given($type){
			when('real'){
				# while(my ($k,$v)=each (%listOfOperand)){
				   # print $k;
				   # my $alterStrCheck="alter table $k drop if exists ev ";
				   # my $alterQryCheck=$dbh_ref->prepare($alterStrCheck);
				   # $alterQryCheck->execute();
				   $tableName=$hashToArray[$i];
				   my $alterStr=" alter table $tableName add column ev text";
				   my $alterQry=$dbh_ref->prepare($alterStr);
				   $alterQry->execute();
				   my $updateStr=" update $tableName set ev='$tableName'||'_'||trunc(random()*99)+1";
				   my $updateQry=$dbh_ref->prepare($updateStr);
				   $updateQry->execute();
                               #}
				
                             }
                    }
                    
         
               
            }
            
              
        }
      }       

my $len=scalar(@query_split);
my $counter=0;
@operandsOfQuery=();
@opeartorsOfQueryS=();
# for( $i=0;$i<$len;$i++){
# print"$query_split[$i]\n";
# }

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
#print $counter;

print"Please enter the corresponding conditions based on Bracket Off from left to right.\n";
@conditions=();
for ($i=0;$i< $counter;$i++){
	chomp(my $input=<>);
	push(@conditions,$input);
}

for ($i=0;$i<scalar(@conditions);$i++){
	#print"$conditions[$i]\n";
	
}
#exit 1;
#print"print the content of the array by use of for loop!!\n";

#print"@conditions\n";

parser($query,$type,\@conditions);
my $precedence=1;
sub parser($$$)
{
	my ($querylocal,$typelocal,$conditionsaddress)=@_;
	my @conditionsArray=@{$conditionsaddress};

	#print"$querylocal,$typelocal,@conditionsArray\n";
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
                 		#print "\n\n\n";
                 		#print"$operatorPope\n";
                 		
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
                       
       #print "@operatorsOfQuery\n";
       #print "@operandsOfQuery\n";
       
  } 

 sub queryEvaluation($$$$$){
 	
 	#print"inside the subroutine!";
 	my($type,$firstOperand,$secondOperand,$operatorPoped,$condition)=@_; 
 	#print"$type,$firstOperand,$secondOperand,$operatorPoped,$condition\n";
 	my $operand=$operatorPoped;
 	schema_join_finder($firstOperand,$secondOperand);
 	#print "$operand\n";
       # print "$queryString\n";
       
 	
        given( $operand ) {
        	  when ( "j"){ 
        	  	given($type){
        	  		when(4){
        	  			  my $operand2=schema_finder_groupby_union($secondOperand);
        	  			  my $operand1=schema_finder_groupby_union($firstOperand);
					  my $joinStr="select * from (select $operand2 from $secondOperand)$secondOperand  join (select $operand1 from $firstOperand )$firstOperand on $condition";
                                          
					  print "$joinStr\n";
					  #exit 0;
					  my $joinQry=$dbh_ref->prepare($joinStr);
					  $joinQry->execute();
					  # print "$joinStr\n";
					  # my $joinschema=schema_join_finder($secondOperand,$firstOperand);
					  my $tableName=table_generator (schema_join_finder($secondOperand,$firstOperand),"temp","join");
    
					  initial_insertion($tableName,$joinStr);
					  #exit 0;
					  update_annotation_join($secondOperand,$firstOperand,$type);
        	  			
        	  			
        	  	               }
        	  	        when (1||2||3){ 
        	  	               
					 my $joinStr="select * from $secondOperand join $firstOperand on $condition";
					 #print "$joinStr\n";
					 my $joinQry=$dbh_ref->prepare($joinStr);
					 $joinQry->execute();
					 # print "$joinStr\n";
					 # my $joinschema=schema_join_finder($secondOperand,$firstOperand);
					 my $tableName=table_generator (schema_join_finder($secondOperand,$firstOperand),"temp","join");
    
					 initial_insertion($tableName,$joinStr);
					 update_annotation_join($secondOperand,$firstOperand,$type);
		                          }
        	              }
        	              
        	              }
                  when ( "u"){
                  	given($type){
        	  		when(4){
        	  			  print "inside union-probabilistic";
        	  			  my $operand2=schema_finder_groupby_union_p($secondOperand);
        	  			  my $operand1=schema_finder_groupby_union_p($firstOperand);
					  my $unionStr="(select $operand1 from $firstOperand) union (select $operand2 from $secondOperand)";
					  print $unionStr;
					  #exit 1;
					  my $unionQry=$dbh_ref->prepare($unionStr);
					  $unionQry->execute();
					  my $tableName=table_generator (schema_finder_union_selection($secondOperand),"temp","union");
					  initial_insertion("temp_union",$unionStr);
					  update_annotation_union($secondOperand,$firstOperand,$type);
        	                          
					}
		           when (1||2||3){
		           	
		           	          my $unionStr="(select * from $firstOperand) union (select * from $secondOperand)";
		           	          
					  my $unionQry=$dbh_ref->prepare($unionStr);
					  $unionQry->execute();
					  my $tableName=table_generator (schema_finder_union_selection($secondOperand),"temp","union");
					  initial_insertion("temp_union",$unionStr);
					  update_annotation_union($secondOperand,$firstOperand,$type); 		
                                    }
                                  }  
                              }
                  my $annotation=();
                  my $annotationType=();
                  when ("p" ){
                  #print $condition;
                  given( $type){
        	          when (1){
        	          $annotation="multiplicity";
        	          $annotationType="int";
        		  } 
        		  when (2){
        		  $annotation="provenance";
        		  $annotationType="text";
        		  }
        		  when(3){
        		  $annotation="uncertainty";
        		  $annotationType="text";
        		  }
        		  when(4){
        		  $annotation="ev";
        		  $annotationType="text";
        		  }	
        		  				
                  }
                  my $modifiedCondition=$condition.",".$annotation;
                  my $projectionStr= "select $modifiedCondition from $firstOperand";
                  #print"\n\n\n";
                  # print $projectionStr;
                  # exit 1;
                  my $projectionQry=$dbh_ref->prepare($projectionStr);
        	  $projectionQry->execute();
        	  my $projectionSchema=schema_finder($firstOperand,$condition);
        	  $projectionSchema=$projectionSchema.", ".$annotation." ".$annotationType;
                 
                  my $tableName=table_generator ($projectionSchema,"temp","projection");
        	  initial_insertion("temp_projection",$projectionStr);
        	  update_annotation_projection($firstOperand,$projectionSchema,$condition,$type);
                  }
                  
                  when ("s" ){
                  my $updatedSchema=schema_finder_groupby_union_p($firstOperand);
                  my $selectStr= "select $updatedSchema from $firstOperand where $condition";
                  #print "$selectStr\n";
                  my $selectQry=$dbh_ref->prepare($selectStr);
        	  $selectQry->execute();
        	  my $tableName=table_generator (schema_finder_union_selection($firstOperand),"$firstOperand","s");
        	  # print $tableName;
        	  # exit 0;
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
        
                    if($schema eq undef && $name ne 'probability') {
                        $schema=$table."_".$name." ".$type;
                    }
                    else {
                    	if( $name ne 'probability'){
                        $schema=$schema.",".$table."_".$name." ".$type;
                        }
                    }
               
        }
        $no++; 
        $table=$table2;     
        }
        #print " $schema,inside schema_join_finder";
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
           
		if($schema eq undef && $name ne 'probability') {
                        $schema=$name." ".$type;
                    }
               else {
               	     if($name ne 'probability'){
                        $schema=$schema.",".$name." ".$type;
                        }
                    }
         }
        #print $schema;
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
		        if ($name ne "multiplicity" && $name ne "provenance" && $name ne "uncertainty"  && $name ne "probability" && $name ne "ev"){
                        $schema=$name;
                        }
                 }
               else {
                        if ($name ne "multiplicity" && $name ne "provenance" && $name ne "uncertainty" && $name ne "probability" && $name ne "ev"){
                        $schema=$schema.",".$name;
                        }
                     }   
         }
        #print $schema;
	return $schema;	
}


sub schema_finder_groupby_union_p($) 
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
		        if ($name ne "multiplicity" && $name ne "provenance" && $name ne "uncertainty"  && $name ne "probability"){
                        $schema=$name;
                        }
                 }
               else {
                        if ($name ne "multiplicity" && $name ne "provenance" && $name ne "uncertainty" && $name ne "probability" ){
                        $schema=$schema.",".$name;
                        }
                     }   
         }
        #print $schema;
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
       # print $createStr;
       # exit 0;
       my $createQry=$dbh_ref->prepare($createStr);
       $createQry->execute();
       print"Inside Table_generator";
       print $tableName;
       
       return($tableName);
       
 }
 
 sub drop_table($)
 {	     
	my ($tableName)=@_;
	my $dropStr="Drop table if exists $tableName  ";
	my $dropQry=$dbh_ref->prepare($dropStr);
	$dropQry->execute();
	#print"Inside Drop Table";
 }   
 
     
sub initial_insertion($$)
{
	my ($tableName,$queryString) = @_;
	my $insertStr="insert into $tableName ($queryString)";

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
        		#print"inside switch\n";
        		my $firstAnno=$firstOperand."_".$annotation;
			my $secondAnno=$secondOperand."_".$annotation;
			my $updateStr="update temp_join set $firstAnno=$firstAnno*$secondAnno";
			my $updateQry=$dbh_ref->prepare($updateStr);
			$updateQry->execute();
			#print"Inside update_annotation_join!\n";
			my $dropClmStr="alter table temp_join drop column $secondAnno ";
			my $dropClmSQry=$dbh_ref->prepare($dropClmStr);
			$dropClmSQry->execute();
			#print"Inside update_annotation_join! DROP COLUMN\n";
			my $renameClmStr="alter table temp_join rename column $firstAnno to $annotation";
			my $renameClmSQry=$dbh_ref->prepare($renameClmStr);
			$renameClmSQry->execute();
        
			#print"Inside update_annotation_join!RENAME COLUMN\n";
        
			my $groupbySchema=schema_finder_groupby_union("temp_join");
			my $tableName=table_generator (schema_finder_union_selection("temp_join"),$firstOperand,$secondOperand);
			my $updateStr=" insert into $tableName ($groupbySchema,multiplicity) (select $groupbySchema,sum ($annotation) from temp_join group by $groupbySchema)";
			#print $updateStr;
			my $updateQry=$dbh_ref->prepare($updateStr);
			$updateQry->execute();
			return($tableName);
                        }
               when (2){
               	        $annotation="provenance";
        		#print"inside switch\n";
        		my $firstAnno=$firstOperand."_".$annotation;
			my $secondAnno=$secondOperand."_".$annotation; 
        	        my $updateStr="update temp_join set $firstAnno =($secondAnno||'*'||$firstAnno)";
			my $updateQry=$dbh_ref->prepare($updateStr);
			$updateQry->execute();
			#print"Inside update_annotation_join!\n";
			my $dropClmStr="alter table temp_join drop column $secondAnno ";
			my $dropClmSQry=$dbh_ref->prepare($dropClmStr);
			$dropClmSQry->execute();
			#print"Inside update_annotation_join! DROP COLUMN\n";
			my $renameClmStr="alter table temp_join rename column $firstAnno to $annotation";
			my $renameClmSQry=$dbh_ref->prepare($renameClmStr);
			$renameClmSQry->execute();
			#print"Inside update_annotation_join!RENAME COLUMN\n";
        
			my $groupbySchema=schema_finder_groupby_union("temp_join");
			my $tableName=table_generator (schema_finder_union_selection("temp_join"),$firstOperand,$secondOperand);
			my $updateStr=" insert into $tableName (select * from temp_join)";
			#print $updateStr;
			my $updateQry=$dbh_ref->prepare($updateStr);
			$updateQry->execute();
			return($tableName);
                       }
               when (3){
               	        $annotation="uncertainty";
        		#print"inside switch\n";
        		my $firstAnno=$firstOperand."_".$annotation;
			my $secondAnno=$secondOperand."_".$annotation; 
			
			
        	        my $updateStr="update temp_join set $firstAnno =($secondAnno||'^'|| $firstAnno)";
			my $updateQry=$dbh_ref->prepare($updateStr);
			$updateQry->execute();
			#print"Inside update_annotation_join!\n";
			my $dropClmStr="alter table temp_join drop column $secondAnno ";
			my $dropClmSQry=$dbh_ref->prepare($dropClmStr);
			$dropClmSQry->execute();
			#print"Inside update_annotation_join! DROP COLUMN\n";
			my $renameClmStr="alter table temp_join rename column $firstAnno to $annotation";
			my $renameClmSQry=$dbh_ref->prepare($renameClmStr);
			$renameClmSQry->execute();
			#print"Inside update_annotation_join!RENAME COLUMN\n";
        
			my $groupbySchema=schema_finder_groupby_union("temp_join");
			my $tableName=table_generator (schema_finder_union_selection("temp_join"),$firstOperand,$secondOperand);
			my $updateStr=" insert into $tableName (select * from temp_join)";
			#print $updateStr;
			my $updateQry=$dbh_ref->prepare($updateStr);
			$updateQry->execute();
			return($tableName);
               	
                      } 
                when (4){
               	        $annotation="ev";
        		#print"inside switch\n";
        		my $firstAnno=$firstOperand."_".$annotation;
			my $secondAnno=$secondOperand."_".$annotation; 
			
			
        	        my $updateStr="update temp_join set $firstAnno =($secondAnno||'^'|| $firstAnno)";
        	        print $updateStr;
        	       
			my $updateQry=$dbh_ref->prepare($updateStr);
			$updateQry->execute();
			#print"Inside update_annotation_join!\n";
			my $dropClmStr="alter table temp_join drop column $secondAnno ";
			my $dropClmSQry=$dbh_ref->prepare($dropClmStr);
			$dropClmSQry->execute();
			
			#print"Inside update_annotation_join! DROP COLUMN\n";
			my $renameClmStr="alter table temp_join rename column $firstAnno to $annotation";
			my $renameClmSQry=$dbh_ref->prepare($renameClmStr);
			$renameClmSQry->execute();
			#print"Inside update_annotation_join!RENAME COLUMN\n";
                        
			my $groupbySchema=schema_finder_groupby_union("temp_join");
			my $tableName=table_generator (schema_finder_union_selection("temp_join"),$firstOperand,$secondOperand);
			
			my $updateStr=" insert into $tableName (select * from temp_join)";
			print $updateStr;
			#exit 0;
			my $updateQry=$dbh_ref->prepare($updateStr);
			$updateQry->execute();
			#exit 0;
			return($tableName);
               	
                      }               
        }
       
     
 }     
        	  
sub update_annotation_union($$$)
{    
        my ($firstOperand,$secondOperand,$type) = @_;
        my $tableName=$firstOperand."_".$secondOperand;
        my $annotation=();
        given( $type){
        	when (1){
        	        $annotation="multiplicity";
        		#print"inside switch\n";
        		my $groupbySchema=schema_finder_groupby_union($firstOperand);
			my $tableName=table_generator (schema_finder_union_selection($firstOperand),$firstOperand,$secondOperand);
			my $updateStr=" insert into $tableName (select $groupbySchema,sum ($annotation) from temp_union group by $groupbySchema)";
			#print $updateStr;
			my $updateQry=$dbh_ref->prepare($updateStr);
			$updateQry->execute();
			#print"Inside update_annotation_union!\n";
			return($tableName);	
			}
	       when (2){
	        	$annotation="provenance";
	        	my $groupbySchema=schema_finder_groupby_union($firstOperand);
	        	my $tableName=table_generator (schema_finder_union_selection($firstOperand),$firstOperand,$secondOperand);
        		my $proStr="select $groupbySchema from temp_union group by $groupbySchema";
        		
			my $proQry=$dbh_ref-> prepare( $proStr);

                        my $modifiedCon=cond_modification($groupbySchema);
			my $selStr="select $annotation from temp_union where $modifiedCon";
			#print "inside update annotation union\n";
		
			my $selQry= $dbh_ref-> prepare( $selStr);
			my $reducedVal;
			my $finalVal;
			$proQry->execute();
			while (@_=$proQry->fetchrow_array)
			{
				$selQry->execute(@_);
				my $value=();
	
				while (my ($anno)=$selQry->fetchrow_array)
				{    
					if ($value eq undef){
					$value=$anno;
					}
					else{
					$value=$value."+".$anno;
					}
				}
				
				$reducedVal=annotation_reducer($value);
                                my $nocolumns=get_question_mark($groupbySchema);
                                my $insertStr="insert into $tableName values ($nocolumns)";
                                #print" inside insertin part\n";
                                my $insertQry=$dbh_ref->prepare ($insertStr);
                                $insertQry->execute(@_,$reducedVal);
                         
                     }
                     return($tableName);	
                   }
                   when (3){
	        	$annotation="uncertainty";
	        	my $groupbySchema=schema_finder_groupby_union($firstOperand);
	        	my $tableName=table_generator (schema_finder_union_selection($firstOperand),$firstOperand,$secondOperand);
        		my $proStr="select $groupbySchema from temp_union group by $groupbySchema";
        		
			my $proQry=$dbh_ref-> prepare( $proStr);

                        my $modifiedCon=cond_modification($groupbySchema);
			my $selStr="select $annotation from temp_union where $modifiedCon";
			#print "inside update annotation union\n";
		
			my $selQry= $dbh_ref-> prepare( $selStr);
			my $reducedVal;
			my $finalVal;
			$proQry->execute();
			while (@_=$proQry->fetchrow_array)
			{
				$selQry->execute(@_);
				my $value=();
	
				while (my ($anno)=$selQry->fetchrow_array)
				{    
					if ($value eq undef){
					$value="(".$anno;
					}
					else{
					$value=$value."v".$anno;
					}
				}
				$value=$value.")";
				
                                my $nocolumns=get_question_mark($groupbySchema);
                                my $insertStr="insert into $tableName values ($nocolumns)";
                                #print" inside insertin part\n";
                                my $insertQry=$dbh_ref->prepare ($insertStr);
                                $insertQry->execute(@_,$value);
                         
                     }
                     return($tableName);	
                   }
                   when (4){
	        	$annotation="ev";
	        	my $groupbySchema=schema_finder_groupby_union($firstOperand);
	        	my $tableName=table_generator (schema_finder_union_selection($firstOperand),$firstOperand,$secondOperand);
        		my $proStr="select $groupbySchema from temp_union group by $groupbySchema";
        		# print $proStr;
        		# exit 0;
        		
			my $proQry=$dbh_ref-> prepare( $proStr);

                        my $modifiedCon=cond_modification($groupbySchema);
                        print $modifiedCon;
                       
                        
			my $selStr="select $annotation from temp_union where $modifiedCon";
			print $selStr;
			#exit 0;
			my $selQry= $dbh_ref-> prepare( $selStr);
			my $reducedVal;
			my $finalVal;
			$proQry->execute();
			while (@_=$proQry->fetchrow_array)
			{
				$selQry->execute(@_);
				my $value=();
	
				while (my ($anno)=$selQry->fetchrow_array)
				{    
					if ($value eq undef){
					$value="(".$anno;
					}
					else{
					$value=$value."v".$anno;
					}
				}
				$value=$value.")";
				
                                my $nocolumns=get_question_mark($groupbySchema);
                                my $insertStr="insert into $tableName values ($nocolumns)";
                                #print" inside insertin part\n";
                                #exit 0;
                                my $insertQry=$dbh_ref->prepare ($insertStr);
                                $insertQry->execute(@_,$value);
                         
                     }
                     return($tableName);	
                   }
                } 
  }

sub update_annotation_projection($$$$)
{    
        my ($firstOperand,$projectionSchema,$condition,$type) = @_;
        my $tableName=$firstOperand."_p";
        my $annotation=();
        given( $type){
        	when (1){
        	        $annotation="multiplicity";
        	        my $tableName=table_generator ($projectionSchema,$firstOperand,"p");
			my $updateStr=" insert into $tableName (select $condition,sum ($annotation) from temp_projection group by $condition)";
			#print $updateStr;
			my $updateQry=$dbh_ref->prepare($updateStr);
			$updateQry->execute();
			#print"Inside update_annotation_union!\n";
			return($tableName);
        	
        		} 
        	when(2){
        	        $annotation="provenance";
        	        my $tableName=table_generator ($projectionSchema,$firstOperand,"p");
        	        my $proStr="select $condition from temp_projection group by $condition";
        	        
			my $proQry=$dbh_ref-> prepare( $proStr);

                        my $modifiedCon=cond_modification($condition);
			my $selStr="select $annotation from temp_projection where $modifiedCon";
			
			my $selQry= $dbh_ref-> prepare( $selStr);
			$proQry->execute();
			my $reducedVal;
			while (@_=$proQry->fetchrow_array)
			{
				$selQry->execute(@_);
				my $value=();
	
				while (my ($anno)=$selQry->fetchrow_array)
				{    
					if ($value eq undef){
					$value=$anno;
					}
					else{
					$value=$value."+".$anno;
					}
				}
				$reducedVal=annotation_reducer($value);
                                my $nocolumns=get_question_mark($condition);
                                my $insertStr="insert into $tableName values ($nocolumns)";
                                my $insertQry=$dbh_ref->prepare ($insertStr);
                                $insertQry->execute(@_,$reducedVal);
                         
                     }
                     return($tableName);		
                    } 
                    when(3){
        	        $annotation="uncertainty";
        	        my $tableName=table_generator ($projectionSchema,$firstOperand,"p");
        	        my $proStr="select $condition from temp_projection group by $condition";
        	        
			my $proQry=$dbh_ref-> prepare( $proStr);

                        my $modifiedCon=cond_modification($condition);
			my $selStr="select $annotation from temp_projection where $modifiedCon";
			
			my $selQry= $dbh_ref-> prepare( $selStr);
		
			$proQry->execute();
			
			while (@_=$proQry->fetchrow_array)
			{
				$selQry->execute(@_);
				my $value=();
	
				while (my ($anno)=$selQry->fetchrow_array)
				{    
					if ($value eq undef){
					$value="(".$anno;
					}
					else{
					$value=$value."v".$anno;
					}
					
				}
				$value=$value.")";
                                my $nocolumns=get_question_mark($condition);
                                my $insertStr="insert into $tableName values ($nocolumns)";
                                my $insertQry=$dbh_ref->prepare ($insertStr);
                                $insertQry->execute(@_,$value);
                         
                     }
                     return($tableName);		
                    } 
                     when(4){
        	        $annotation="ev";
        	        my $tableName=table_generator ($projectionSchema,$firstOperand,"p");
        	        my $proStr="select $condition from temp_projection group by $condition";
        	        
			my $proQry=$dbh_ref-> prepare( $proStr);

                        my $modifiedCon=cond_modification($condition);
                        print $modifiedCon;
                        #exit 0;
			my $selStr="select $annotation from temp_projection where $modifiedCon";
			
			my $selQry= $dbh_ref-> prepare( $selStr);
		
			$proQry->execute();
			
			while (@_=$proQry->fetchrow_array)
			{
				$selQry->execute(@_);
				my $value=();
	
				while (my ($anno)=$selQry->fetchrow_array)
				{    
					if ($value eq undef){
					$value="(".$anno;
					}
					else{
					$value=$value."v".$anno;
					}
					
				}
				$value=$value.")";
                                my $nocolumns=get_question_mark($condition);
                                my $insertStr="insert into $tableName values ($nocolumns)";
                                my $insertQry=$dbh_ref->prepare ($insertStr);
                                $insertQry->execute(@_,$value);
                         
                     }
                     return($tableName);		
                    } 
                    
                    
                        
  }
}
sub cond_modification($) {

    my ($cond)=@_;
    my @con_arr=split(',',$cond);
    my $new_cond=();
    $new_cond=$con_arr[0]." = ?";
    my $modif_con=$new_cond;

    for(my $k=1; $k<scalar(@con_arr);$k++) {
        $new_cond=$con_arr[$k]." = ?" ;
        $modif_con = $modif_con." and ".$new_cond;
        }
    return $modif_con;

}
sub get_question_mark($) {

    my ($cond)=@_;
    my @con_arr=split(',',$cond);

    my $question = "?";
    #print scalar(@con_arr);

    for(my $k=0; $k<scalar(@con_arr);$k++) {
        $question=$question.",?";
        }
    return $question;
}

sub annotation_reducer($){
my($value)=@_;
my @valueArray=split('\+',$value);
my $newValue;
my $i=0;
my %myhash=();
 while ( $i<scalar(@valueArray)){
	if ($myhash{$valueArray[$i]} eq undef){
		$myhash{$valueArray[$i]}=1;
	}
	else{
		$myhash{$valueArray[$i]}=$myhash{$valueArray[$i]}+1;
	    }	
$i++;
}
    
while (my($key,$value)=each(%myhash))
{
	if ($newValue eq undef){
	   given($value){
	   	when (1){	
	           $newValue=$key;
	           }
	        default{
	           $newValue=$value.$key;	
	           }   
	           }
       }
	else{
		given($value){
	   	when (1){	
	           $newValue=$newValue.'+'.$key;
	           }
	       default{
	           $newValue=$newValue.'+'.$value.$key;	
	           }   
	           }
            	
            }	   
}
#print $newValue;
return($newValue);
}

sub annotation_reducer_join($){
my($value)=@_;

my @valueArray=split('\*',$value);
my $newValue;
my $i=0;
my %myhash=();
 while ( $i<scalar(@valueArray)){
	if ($myhash{$valueArray[$i]} eq undef){
		$myhash{$valueArray[$i]}=1;
	}
	else{
		$myhash{$valueArray[$i]}=$myhash{$valueArray[$i]}+1;
	    }	
$i++;
}
while (my($key,$value)=each(%myhash))
{
    #print $key,$value;
    #print "\n";
    }

while (my($key,$value)=each(%myhash))
{
	if ($newValue eq undef){
	   given($value){
	   	when (1){	
	           $newValue=$key;
	           }
	        default{
	           $newValue=$key.'^'.$value;	
	           }   
	           }
       }
	else{
		given($value){
	   	when (1){	
	           $newValue=$newValue.'*'.$key;
	           }
	       default{
	           $newValue=$newValue.'*'.$key.'^'.$value;	
	           }   
	           }
            	
            }	   
}
#print $newValue;
return($newValue);
}

sub EventVarGen($) {

    my ($tableName)=@_;
    my $range = 200;

    my $random_number = int(rand($range));
    my $ev="r".$random_number;

    print $ev;
    return($ev);
    
    }
sub operand_finder($) {
my ($query) = @_;
#chomp(my $query=<>);
my @query_split = split /[ j | s | u | p | \)| \( |\s* ]/,$query;
my %value=();
# print @query_split;
# exit; 
for ($i=0;$i<scalar(@query_split);$i++){
   if($query_split[$i] ne ' ' && $query_split[$i] ne undef) {
	$value{$query_split[$i]}=' ';
	}
}	

 # while(my($k,$v)=each(%value)) {
    # print "$k: $v\n";	
# }
return(\%value);
}
	 
