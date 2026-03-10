import subprocess

def total(total,parts):
    range_of_files=int(total/parts)
    for i in range(1,total,range_of_files):
        start = i
        end  = i + range_of_files
        command = "start python parsel.py " + str(start) + " " + str(end)
        with open('run.bat', 'a') as f:
            f.write(command+'\n')
        # subprocess.run(command)
total(130000,16)