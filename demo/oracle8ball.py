from mysqlsh.plugin_manager import plugin, plugin_function
from random import randrange

@plugin_function("demo.oracle8ball")
def tell_me():
    """
    Function that prints the so expected answer

    What's the answer ? 

    Returns:
        a string
    """
    answers = ["It is certain.", "It is decidedly so.","Without a doubt.",
               "Yes - de nitely.", "You may rely on it.",
               "As I see it, yes.","Most likely.","Outlook good.",
               "Yes.","Signs point to yes.","Reply hazy, try again.","Ask again later.",
               "Better not tell you now.","Cannot predict now.",
               "Concentrate and ask again.", "Don't count on it.",
               "My reply is no.","My sources say no.",
               "Outlook not so good.","Very doubtful."]
    print(answers[randrange(20)])
