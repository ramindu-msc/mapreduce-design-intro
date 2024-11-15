REGISTER $piggybank_jar;

-- Define the XMLLoader with no arguments
DEFINE MyXMLLoader org.apache.pig.piggybank.storage.XMLLoader();

-- Load the XML file using the defined XMLLoader
comments = LOAD '/user/test/stackapps.com/comments/Comments2.xml'
    USING org.apache.pig.piggybank.storage.XMLLoader(
        'Id:int,PostId:int,Score:int,Text:chararray,CreationDate:chararray,UserId:int'
    );

Lines = LOAD '/user/test/stackapps.com/comments/Comments2.xml' AS (line: chararray); 
Words = FOREACH Lines GENERATE  FLATTEN(TOKENIZE(line)) AS word;
Groups = GROUP Words BY word;
Counts = FOREACH Groups GENERATE  group, COUNT(Words) AS word_count; 
Results = ORDER Counts BY word_count DESC;
Top5 = LIMIT Results 5;
STORE Top5 INTO '/user/test/top5words';