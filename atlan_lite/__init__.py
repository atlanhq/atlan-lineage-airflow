
def set_inputs(inputs, op):
    for i in inputs:
        op.inlets = inputs

def set_outputs(outputs, op):
    for o in outputs:
        op.outlets = outputs


# inlet > operator > outlet 

# >> operator > outlet >> operator
class Test:
    def __init__(self, lhs):
        self.lhs = lhs
    
    
    def __gt__(self, rhs):
        if 'Operator' in self.lhs.__class__.__name__:
            print("LHS is operator")
            if type(rhs) == 'str':
                outputs = [rhs.lhs]
            else:
                outputs = rhs.lhs
            self.lhs.outlets = outputs
        # elif rhs.lhs.__class__.__name__ == 'B':
        elif 'Operator' in rhs.lhs.__class__.__name__:
            print("RHS is operator")
            if type(self.lhs) == 'str':
                inputs = [self.lhs] 
            else:
                inputs = self.lhs
            rhs.lhs.inlets = inputs
        else:
            print("pass")
            pass
        return self
        
class init:
    def __init__(self, exp):
        self.exp = exp
        self.parse()
        
    def parse(self):
        ## and other validation
        l = self.exp.split(">")
        l =  [Test(eval(l_)) for l_ in l]
        l[0] > l [1]
        