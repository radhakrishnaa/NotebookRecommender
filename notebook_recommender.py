import sys
import json
from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED
from whoosh.analysis import StemmingAnalyzer
from whoosh import index
from whoosh.qparser import QueryParser
from whoosh import scoring
import os, os.path


#####################################
#Create the schema
#####################################
schema = Schema(filename=ID(stored=True),
                cell_no=TEXT(stored=True),
                content=TEXT(analyzer=StemmingAnalyzer())
               )


#####################################
# Create the index and initialize a `writer`
#####################################

# Note, this clears the existing index in the directory
ix = index.create_in("notebooks", schema)

# Get a writer form the created index in 
writer = ix.writer()


def visibleTextFromNB(filename):
    '''
    This function pulls all the non-output visible cells from
    a JupyterNotebook and concatenates it all into a block of
    text.
    Returns : a list of the cells
    '''
    #####################################
    # Parse file, pull cells
    #####################################

    file_data = json.load(open(filename, encoding="utf-8"))

    # File data is now a map, recall a JSON format is a combo of dictionaries and lists
    cells = file_data.get('cells')

    #####################################
    # Append cells into a list of cells
    #####################################

    cell_list = []
    if cells == None:
        return cell_list
    
    # for each cell in the notebook
    for c in cells:

        #extract and test the cell type
        cell_type = c['cell_type']
        if ('code'==cell_type or 'markdown'==cell_type or 'raw'==cell_type ):
            cell_text = ""
            # run the source into lines, it is actually a list of strings/lines
            source = c['source']
            for l in source:
                cell_text += l
            cell_list.append(cell_text)

            
    #####################################
    # Append cells into a list of cells
    #####################################

    # return the list
    return cell_list

#End of function: visibleTextFromNB 

def loadFile(writer, fname):
    '''
    Read file contents, load into database.
    '''
    #####################################
    # Get cell text from function
    #####################################

    cells = visibleTextFromNB(fname) 
    
    
    #####################################
    # Iterate through cells, index
    #####################################

    counter = 1;
    for c in cells:
        writer.add_document(filename=fname, cell_no=str(counter), content=c) ### ANSWERS: your code here
        counter +=1   
#     print("Indexed: ", fname)

# END of function

def walkFolder(writer, folder):
    '''
    Process a folder for files and subfolders
    Prints the files and folders that are processed. 
    '''
    #print('Processing folder: ',folder)
    
    #####################################
    # TODO: walk through the filesystem starting at folder
    # HINT: os.walk
    #####################################

    for root, dirs, files in os.walk(folder):
        #print("root = ", root)
        
        result = []
        for d in dirs:
            if (d.startswith(".") or d== 'share' or d=='jupyter' or d == 'runtime' or ("collection" in d) or ("grading" in d) or (d == "PSDS2120") or (d == "extracted")):
                pass
            else:
                result.append(d)
                
        dirs[:] = result
        
        #####################################
        # Process Files
        #####################################
        for file in files:
            filename = os.path.join(root, file)
            if file.endswith("-checkpoint.ipynb"):
                pass
            elif file.endswith(".ipynb"):
                print('Found Notebook:',filename)
                loadFile(writer, filename)
############# END for walkFolder

walkFolder(writer,"C:\DSA")


# Commit changes
writer.commit() # save changes

# Get input, conver to unicode
qstr = input("Input a qeury: ")

print("searching for ",qstr)

####################################
# Build query parser and parse query
####################################
qp = QueryParser("content", schema=ix.schema)
q = qp.parse(qstr)

print(q)

####################################
# Search the content field
####################################
with ix.searcher(weighting=scoring.TF_IDF()) as s:
    results = s.search(q)
    for hit in results:
        print("Cell {} of Notebook '{}'".format(hit['cell_no'],hit['filename']))