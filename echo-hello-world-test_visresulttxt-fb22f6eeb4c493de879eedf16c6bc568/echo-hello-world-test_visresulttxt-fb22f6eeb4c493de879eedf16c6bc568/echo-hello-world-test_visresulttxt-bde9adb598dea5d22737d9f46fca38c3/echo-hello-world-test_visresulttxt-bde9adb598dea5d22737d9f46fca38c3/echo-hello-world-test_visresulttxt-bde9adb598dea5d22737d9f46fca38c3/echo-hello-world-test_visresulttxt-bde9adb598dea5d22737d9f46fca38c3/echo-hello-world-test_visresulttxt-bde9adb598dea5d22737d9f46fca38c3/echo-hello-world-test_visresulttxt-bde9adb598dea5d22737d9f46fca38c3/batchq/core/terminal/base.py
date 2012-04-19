def convert_to_int(x):
    try:
        return int(x)
    except:
        return x

class BaseInterpreter(object):
    """
    This object implements the base interpretater.
    """
    def __init__(self,escape_register, key_register, pattern_reg = None,  max_cols = 80, max_rows = 80):
        self._max_cols = max_cols
        self._max_rows = max_rows
        self.erase_display(p = 2)
        self._buffer = ""
        self._escpat = False
        self._escape_patterns = escape_register.re
        self._KeyPatterns = key_register.re
        self._escape_register = escape_register
        self._key_register = key_register
        self._pattern_reg = pattern_reg
        self._last_monitor_char = 0
        self._last_monitor_line = 0
        self._mark_line = 0
        self._mark_char = 0
        self._full_echo = ""
        self._mark_echo = 0
        self._last_monitor_echo = 0
        self._echo_position = 0
        self._curline = 0
        self._curchar = 0
        self._lines = []
        self._attributes = {}
        self._last_ret = ""
        self._home_line = 0
        self._home_char = 0
        self._home_attributes = {}
        self._debug = False
        self._carriage_return = 0


        if not pattern_reg is None: pattern_reg.set_owner(self)

        self.fix_buffer()
        

    def move_monitor_cursors(self):
        self._last_monitor_echo = self._echo_position
        self._last_monitor_char = self._curchar
        self._last_monitor_line = self._curline


    @property
    def monitor_echo(self):
        ret = self._full_echo[self._last_monitor_echo:self._echo_position]
        self._last_monitor_echo = self._echo_position
        return ret
    
    @property
    def escape_mode(self):
        return self._escpat

    @property
    def monitor(self):

        if self._curline < self._last_monitor_line: 
            return ""
        if self._curline == self._last_monitor_line and self._curchar < self._last_monitor_char: 
            return ""

        n = len(self._lines)
        buf = "\r\n".join(self._lines[self._last_monitor_line:(self._curline+1)])
        lenlastl = len(self._lines[self._curline])
        n = len(buf) - lenlastl + self._curchar
        ret = buf[self._last_monitor_char:n]

        self._last_monitor_char = self._curchar
        self._last_monitor_line = self._curline

        return ret
        
    @property
    def buffer(self):
        return ("\n".join(self._lines)).replace("\r\n","")

    @property
    def last_escape(self):
        ret = ""
        n = len(self._full_echo)
        i = n-1
        while  i>=0 and self._full_echo[i] != u"\x1b":
            i-=1

        if i>=0:
            i+=1
#            print i
#            print self._full_echo[i:]
            ret ="<ESC>"
            ret+=self._full_echo[i:]
        return ret


    @property
    def echo(self):
        return self._full_echo

    @property
    def readable_echo(self):
        pattern_start = {u'\x00':'NUL', u'\x01':'SOH',u'\x02':'STX',u'\x03':'ETX',u'\x04':'EOT',u'\x05':'ENQ',u'\x06':'ACK',u'\x07':'BEL',
                         u'\x08':'BS',u'\x09':'TAB',u'\x0A':'NL',u'\x0B':'VT',u'\x0C':'NP',u'\x0D':'CR',u'\x0E':'SO',u'\x0F':'SI',u'\x10':'DLF',
                         u'\x11':'DC1',u'\x12':'DC2',u'\x13':'DC3',u'\x14':'DC4',u'\x15':'NAK',u'\x16':'SYN',u'\x17':'ETB',u'\x18':'CAN',u'\x19':'EM',
                         u'\x1A':'SUB',u'\x1b': 'ESC',u'\x1C':'FS',u'\x1D':'GS',u'\x1E':'RS',u'\x1F':'US'}
        basic_reductions = {'ESC [': 'CSI', 'ESC D': 'IND', 'ESC E': 'NEL', 'ESC H': 'HTS', 'ESC M': 'RI', 'ESC N': 'SS2','ESC O': 'SS3',
                            'ESC P': 'DCS', 'ESC V': 'SPA', 'ESC W':'EPA', 'ESC X': 'SOS', 'ESC Z':'CSI c', "ESC \\": 'ST', 'ESC ]':'OSC',
                            'ESC ^': 'PM', 'ESC _':'APC'}
        echo = self._full_echo
        for a,b in pattern_start.iteritems():
            echo = echo.replace(a,b+" ")

        for a,b in basic_reductions.iteritems():
            echo = echo.replace(a,"<"+b+">")

        for a,b in pattern_start.iteritems():
            echo = echo.replace(b+" ","<"+b+">")
        
        return echo.replace("<NL>", "\n")


    def write(self, str):
        for c in str:
            self.writeChar(c)


    def writeChar(self,c):
        self._full_echo += c
        self._echo_position += 1
        self._buffer+= c

        if self._pattern_reg.parse(c):
            pass
        else:
            self.put(c)
#            print c, 

    @property
    def position(self):
        return self._curchar,self._curline

    def set_mark(self):
        self._mark_line = self._curline
        self._mark_char = self._curchar
        self._mark_echo = self._echo_position

    def copy(self):
#        if self._curline < self._last_monitor_line: return ""
#        if self._curline == self._last_monitor_line and self._curchar < self._last_monitor_char: return ""

        n = len(self._lines)
        buf = "\n".join(self._lines[self._mark_line:n]).replace(u"\r\n","")
        n = len(buf)
        return buf[self._mark_char:n]

    def copy_until_end(self,char, line):
        n = len(self._lines)
        buf = ("\n".join(self._lines[line:n])).replace(u"\r\n","")
        n = len(buf)
        return buf[char:n]



    def copy_echo(self):
        return self._full_echo[self._mark_echo:self._echo_position]


    def fix_buffer(self):
        n = len(self._lines)
        if self._curline+1 > n: self._lines += [""]*(self._curline - n + 1)
        if self._curline < 0: self._curline = 0
#        print self._curline, len(self._lines)
        n = len(self._lines[self._curline])
        if self._curchar<0: self._curchar = 0
        if self._curchar>n: self._lines[self._curline] += " "*(self._curchar - n + 1)


    def put(self,c):
        n = self._curchar
        if self._curchar - self._carriage_return >= self._max_cols:
            self._lines[self._curline] = self._lines[self._curline][0:n] + u"\r" # + self._lines[self._curline][n+1:m]
#            self._carriage_return = self._max_cols*int(self._curchar / self._max_cols)
            # TODO: Only if wordwrap enabled
            self._curchar = 0
            self._curline +=1
            self.fix_buffer()

        m = len(self._lines[self._curline])
        self._lines[self._curline] = self._lines[self._curline][0:n] + c + self._lines[self._curline][n+1:m]
#        print self._curchar, self._lines
        self._curchar+=1
        self.fix_buffer()
