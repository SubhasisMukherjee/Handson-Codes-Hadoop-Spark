/* Load file */
file = LOAD 'datasets/pigdata/dropbox-policy.txt' AS (line);

/* Create relation of bags, where words of a line is made into tuples and all those tuples are made into a bag, 
so a bag would be there for each line */
words = FOREACH file GENERATE TOKENIZE(line) AS word;
dump words;
({(Dropbox),(Privacy),(Policy)})
({(Last),(Modified:),(April),(10),(2013)})
()
({(This),(Privacy),(Policy),(provides),(our),(policies),(and),(procedures),(for),(collecting),(using),(and),
(disclosing),(your),(information.),(Users),(can),(access),(the),(Dropbox),(service),(the),(“Service”),(through),
(our),(website),(www.dropbox.com),(applications),(on),(Devices),(through),(APIs),(and),(through),(third-parties.),
(A),(“Device”),(is),(any),(computer),(used),(to),(access),(the),(Dropbox),(Service),(including),(without),(limitation),
(a),(desktop),(laptop),(mobile),(phone),(tablet),(or),(other),(consumer),(electronic),(device.),(This),(Privacy),(Policy),
(governs),(your),(access),(of),(the),(Dropbox),(Service),(regardless),(of),(how),(you),(access),(it),(and),(by),(using),
(our),(Services),(you),(consent),(to),(the),(collection),(transfer),(processing),(storage),(disclosure),(and),(other),
(uses),(described),(in),(this),(Privacy),(Policy.),(All),(of),(the),(different),(forms),(of),(data),(content),(and),
(information),(described),(below),(are),(collectively),(referred),(to),(as),(“information.”)})

/* Flatten the whole text into one line and split all the words into tuples, so that there would only be tuples, 
no bags for lines */
words = FOREACH file GENERATE FLATTEN(TOKENIZE(line)) AS word;
dump words;
(Dropbox)
(Privacy)
(Policy)
(Last)
(Modified:)
(April)
(10)
(2013)
()
(This)
(Privacy)
(Policy)
(provides)
(our)
(policies)
(and)
(procedures)


/* Generate a relation with group of words, the first element is the word for which grouping is done, 
the second element is the bag consisting of all the occurances of the word */
grouped  = GROUP words BY word;
DUMP grouped;
(.,{(.),(.),(.),(.),(.),(.),(.),(.)})
(A,{(A),(A)})
(a,{(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a),(a)})
(b,{(b)})
(c,{(c)})
(d,{(d)})
(i,{(i)})
(v,{(v)})
(1.,{(1.)})
(10,{(10)})
(13,{(13)})
(2.,{(2.)})
(3.,{(3.)})
(30,{(30)})
(4.,{(4.)})
(5.,{(5.)})
(6.,{(6.)})
(7.,{(7.)})
(8.,{(8.)})
(9.,{(9.)})
(As,{(As),(As)})
(By,{(By)})
(CA,{(CA)})
(ID,{(ID),(ID)})
(IP,{(IP),(IP)})
(If,{(If),(If),(If),(If),(If),(If),(If),(If),(If),(If),(If),(If),(If),(If),(If),(If),(If),(If),(If)})
(In,{(In),(In),(In)})
(No,{(No)})
(S3,{(S3),(S3)})
(To,{(To),(To)})
(Us,{(Us)})
(We,{(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We),(We)})
(an,{(an),(an)})
(as,{(as),(as),(as),(as),(as),(as),(as),(as),(as),(as),(as),(as),(as),(as),(as)})
(at,{(at),(at),(at),(at),(at),(at),(at),(at),(at)})
(be,{(be),(be),(be),(be),(be),(be),(be),(be),(be),(be),(be),(be),(be),(be),(be)})
(by,{(by),(by),(by),(by),(by),(by),(by),(by),(by),(by),(by),(by),(by),(by),(by),(by)})
(do,{(do),(do),(do),(do),(do),(do),(do),(do),(do),(do)})
(he,{(he)})
(if,{(if),(if),(if),(if),(if),(if),(if)})
(ii,{(ii)})
(in,{(in),(in),(in),(in),(in),(in),(in),(in),(in),(in),(in),(in),(in),(in),(in),(in),(in),(in),(in)})
(is,{(is),(is),(is),(is),(is),(is),(is),(is),(is),(is)})
(it,{(it),(it),(it),(it)})
(iv,{(iv)})
(no,{(no),(no),(no)})

/* Generate a relation where for all rows, the first element is the grouping word, the second element is count of all the
word's occurances */
counted  = FOREACH grouped GENERATE group, COUNT(words);
DUMP counted;
(.,8)
(A,2)
(a,26)
(b,1)
(c,1)
(d,1)
(i,1)
(v,1)
(1.,1)
(10,1)
(13,1)
(2.,1)
(3.,1)
(30,1)
(4.,1)
(5.,1)
(6.,1)
(7.,1)
(8.,1)
(9.,1)
(As,2)
(By,1)
(CA,1)
(ID,2)
(IP,2)
(If,19)
(In,3)
(No,1)
(S3,2)
(To,2)
(Us,1)
(We,26)
(an,2)
(as,15)
(at,9)
(be,15)
(by,16)
(do,10)
(he,1)
(if,7)
(ii,1)
(in,19)
(is,10)
(it,4)
(iv,1)
(no,3)

/* Sort the relation 'counted' by the grouping element */
sorted_counted = ORDER counted BY $1;

/* Print the word count, sorted above */
DUMP sorted_counted;



