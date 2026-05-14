import time

"""
This module provides utilities for collect and list timestamps along the execution of a script.

Functions:
- `setcontext(description)`: Sets the description of the running script.
- `getts()`: get the current timestamp.
- `tstart=clts.getts()`: sets the tstart timestamp for further calculations
- `deltat(ts)`: computes time difference (watch and processor) to a given timestamp.
- `elapt['description']=deltat(tstart)`: adds the line `description` and elapsed times to the `table`  
- `listtimes()`: lists the actual table of times colected (ASCII)
- `toem=listtimes()`: toem variable contains a html version of the table (to be sent through email)
Usage:
    import clts_pcp
    tstart=clts_pcp.getts()
    clts_pcp.elapt["step 1 (successful)."]=clts_pcp.deltat(tstart)
    toem=clts_pcp.listtimes()
"""


elapt={}

def listtimes():
    """
    Function: Lists a text-formatted table and returns the html version
    Usage:
    - clts_pcp.listtimes() - prints the table in stdio
    - toem=clts_pcp.listtimes() - prints the table in stdio and returns the html version
    """
    global context
    try:
        context # does a exist in the current namespace
    except NameError:
        context ='Please define `context` variable.'

    cols=[90, 19, 19]
    sepa="+"
    for c in cols:
      sepa=sepa+"-"*c+"+"
 
    print(sepa)
    topp= '|' + f" Task(s) by {context:<76s} " + ' | watch time (secs) |  proc time (secs) |'
    print(topp)
    print(sepa)

    toem=f"<table style=''font-family:montserrat;'' border=1 cellspacing=0><tr background=`whitesmoke` style=`font-weight:bold;`><td> Task(s) of {context}. <td align=center> watch time (secs) <td align=center>proc time (secs)"

    for k in elapt.keys():                                   # dict elapt contains a series of entries key -> elapsed time
        topp = f'| {k:88s} | {elapt[k]["tt"]:17.2f} | {elapt[k]["tp"]:17.2f} |'
        print (topp)
        toem=toem+f"<tr><td>{k}<td align=right>{elapt[k]['tt']:.2f}<td align=right>{elapt[k]['tp']:.2f}"

    print(sepa)
    toem +="</table>"
    return(toem)


def getts():
    """
    Function: return a double timestamp (watch time and processor time)    
    Usage:
    - tstart = clts_pcp.getts()
    """
    return({'tt':time.time(), 'tp':time.process_time()})

def deltat(p):
    """
    Function: return a double timestamp difference (watch time and processor time) from a previous tstamp
    Usage:
    - clts_pcp.elapt["step 2 (successful)."] = clts_pcp.deltat(tstart)
    """
    return({'tt':time.time()-p['tt'], 'tp':time.process_time()-p['tp']})

def setcontext(p):
    """
    Function: sets the `context` (table first row)
    Usage:
    - clts_pcp.setcontext["free text."] = clts_pcp.deltat(tstart)
    """

    global context
    context=p
