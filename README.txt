Environment Installation:
1. To have default roles which match EMR create Cluster and terminate it. you can look on the Roles and see: EMR_DefaultRole and EMR_EC2_DefaultRole
2. Create bucket for logs and edit AWSservice the AWSservice.LOGS_BUCKET_NAME variable
3. Add the name of your key pair in KEY_PAIR variable
4. Create access key and add the access key Id and secret key in c:\users\username\.aws\credentials the values. for example-
	aws_access_key_id =sharon
	aws_secret_access_key =si
5. Create bucket and save the name in AWSservice.OUTPUT_BUCKET_NAME variable
6. Create jar folder and put there all hadoop MapReduce jar files and save the name bucket in AWSservice.JAR_BUCKET_NAME variable.
7. Create bucket to save the N value and save the name bucket in AWSservice.N_FILE_BUCKET

Project logic:
Our main jar includes job flow with includes job flow steps. Every job flow step is Hadoop Map-Reduce jar. 
These are the following map-reduce steps in order:
1. Description: for each 3grams, write the total instances in the corpus.
	map input - google 3grams 
	map method result - 
		key: w1w2w3
		value: 1
	reduce result : 
		key : w1w2w3
		value : r = c(w1w2w3)
	In addition we count the amount of 3grams in all corpus and save it in s3 
2. Description: divide the corpus to two parts
	setup map: declare group variable which is 0/1. first we initialize it to 0.
	map input - google 3grams 
	map method - 
		group = !group;
		key: <w1w2w3, group>
		value: 1
	reduce result : 
		key : <w1w2w3, group>
		value : num instances in of w1w2w3 in specific group (0/1).
3. Description: join results from mapreduce 1 and 2
	map1 input - the results from 1. : key w1w2w3 value: r
	map1 method - 
		emit 2 values one for each group:
			a. key :<w1w2w3, 0>
		     	   value: {‘R’,r-value}
			b. key :<w1w2w3, 1>
			   value: {‘R’,r-value}
	map2 input: the results from 2.
	map2 metod -
		emit 2 values one for group and second for rgroup (rgourp=num instances in of w1w2w3 in specific group)
			a. key: <w1w2w3, group>
		    	   value: {‘G’,group}
			b. key: <w1w2w3, group>
			   value: {‘RG’,rgoup}
	reduce method : recognize the tags and add all values in one text result
	reduce result :
		key :  w1w2w3
		value : rvalue, group, rgroup
4. Description: calculate  Tr01 and Tr10 which exits in the groups 
	map input - results from 3.
	map method result - 
		key: <group , rgroup>
		value: rvalue-  rgroup (num instances in group)
	reduce result : 
		key : <group , rgroup>
		value : the value of Tr10/Tr01 
		(when r=num instances and the first number is the group and the second number is !group)
5. Description: calculate N0r and N1r
	map input - results from 2.
	map method result - 
		key: <group , rgroup>
		value: 1
	reduce result - 
		key : <group , rgroup>
		value : the value of  N0r/ N1r (when r=rgroup)
6. Description: calc probability from the values: N0r, N1r, T01, T10, N (the N it takes from the same bucket in s3 where in step 1. we save it)
	map1 input - results from 4.
	map1 method result - 
		key: rgroup
		value: {‘T’group!group: t-value} (the tag is T01/T10 according to the group) 
	map2 input - results from 5.
	map2 method result - 
		key: rgroup
		value: {‘N’group: n-value} (the tag is N0/N1 according to the group) 
	reduce method - calc probability: (T01+T10)/(N*(N0+N1))
	reduce result - 
		key : rgroup
		value : probability result
7. Description: for each word match the correct probability according to his r-value
	Pay Attention: to the first map we put to the key value 0 to ensure it will came first with the probability and than the all words with the key <r,1> (same r)
			we did it with partitioner - ensure same r the keys <r,0> and <r,1> will be in the same reducer and the compare of the key ensure the <r,0> will be the first.
	map1 input - results from 1.
	map1 method result - 
		key: r,1
		value: {‘id’: w1w2w3} 
	map2 input - results from 6.
	map2 method result - 
		key: rgroup,0
		value: {‘result’:probability value}
	reduce method- we accept first key <rgroup,0> and than key <r,1> when (rgroup = r). 
			if the second value of the key is 0 we save the result and the last rgroup in lastR variable
			if the lastR don't match to the value r in <r,1> key we save the word with 0 probability (means for this r we didn't got <rgroup,0> when rgroup = r). 
	reduce result - 
		key : w1w2w3
		value : probability result
8. Description: sort the output from 7 first by w1w2, ascending, second by the probability for w1w2w3, descending.
	we make WritableComparable class (for the key in the map) which contains 2 properties:
	a. text 
	b. double
	this class has compare method which first according to the text property and then according to double property but * (-1) to get descending order.
	map input - results from 7.
	map method result - 
		key: <w1w2 , probability value>
		value: w1w2w3
	reduce result - 
		key : w1w2w3
		value : probability value
