import json

#
# Resolve KreisID zu Name
#


def kreisid(id):
    if len(id) < 5:
        id = "0"+id
    file = open("data/kreisid.json", "r+")
    data = json.load(file)
    file.close()
    for row in data:
        if row == id:
            return str(data[row][0][0])
    return ""