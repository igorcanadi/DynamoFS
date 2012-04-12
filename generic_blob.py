" " " 
GenericBlob will be a tuple of (type, blob) which will
represent every blob we store on the server.
Type -1 - Uninitialized
Type 0 - Directory Blob
Type 1 - Block List Blob
Type 2 - Block Blob

" " " 

class GenericBlob:
    type = -1
    blob = ""

