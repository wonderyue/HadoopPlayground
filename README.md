# Common Friend

## Input:
Input files 
1. soc-LiveJournal1Adj.txt
The input contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>
Hence, each line represents a particular user’s friend list separated by comma.
2. userdata.txt 
The userdata.txt contains dummy data which consist of 
column1 : userid
column2 : firstname
column3 : lastname
column4 : address
column5: city
column6 :state
column7 : zipcode
column8 :country
column9 :username
column10 : date of birth.

## Mutual/Common friend list of two friends
The key idea is that if two people are friend then they have a lot of mutual/common friends. This program will find the common/mutual friend list for them.

For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy

As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this case you may exclude them from your output). 

### Input arguments
/input/soc-LiveJournal1Adj.txt /output 0,1\;20,28193\;1,29826\;6222,19272\;28041,28056

## Maximum common friends
Find friend pair(s) whose number of common friends is the maximum in all the pairs. 

### Input arguments
/input/soc-LiveJournal1Adj.txt /output

## Mapper join of common friends
Given any two Users (they are friend) as input, output the list of the names and the date of birth (mm/dd/yyyy) of their mutual friends.
userdata.txt will be used to get the extra user information and cached/replicated at each mapper.

### Input arguments
/input/soc-LiveJournal1Adj.txt /output /input/userdata.txt 18666 18668

## Reducer join of common friends
For each user print User ID and maximum age of direct friends of this user.

### Input arguments
/input /output
