
class UserEntry:
    def __init__(self, pwd_string):
        elements = pwd_string.split(':')
        user, password, uid, gid, info, *rest = elements
        self.user = user
        self.uid =  uid
        self.gid = gid
        try:
            self.name, self.assoc, self.email = info.split(',')
        except:
            self.name, self.assoc, self.email = None, None, None

    def __repr__(self):
        return 'User({}, {})'.format(self.name, self.user)

class UserTable:
    def __init__(self):
        self.directory = {}
        with open("/etc/passwd") as fp:
            users = fp.read().splitlines()
            for user in users:
                ue = UserEntry(user)
                if int(ue.uid) > 1000:
                    self.directory[ue.user] = ue
    def __getitem__(self, user):
        return self.directory[user]
