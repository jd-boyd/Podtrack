for f in `ls` ; 
do 
    NEW_NAME=`echo $f | sed 's/\?.*$//'` 

    if [ $f == $NEW_NAME ] ; then
	echo Not touching $f
    else
	if [ -f $NEW_NAME ] ; then
	    echo $NEW_NAME already exists
	else
	    mv $f $NEW_NAME
	fi
    fi
done