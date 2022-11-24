# [...] # Some boring imports
# Mapping from topics to colors
# See the disclaimer at the end of the post if you
# want to use all RGB colors
TOPICS = {
    "TIMR": "bright_black",
    "VOTE": "bright_cyan",
    "LEAD": "yellow",
    "TERM": "green",
    "LOG1": "blue",
    "LOG2": "cyan",
    "CMIT": "magenta",
    "PERS": "white",
    "SNAP": "bright_blue",
    "DROP": "bright_red",
    "CLNT": "bright_green",
    "TEST": "bright_magenta",
    "INFO": "bright_white",
    "WARN": "bright_yellow",
    "ERRO": "red",
    "TRCE": "red",
}

# [...] # Some boring command line parsing

# We can take input from a stdin (pipes) or from a file
input_ = file if file else sys.stdin
# Print just some topics or exclude some topics
if just:
    topics = just
if ignore:
    topics = [lvl for lvl in topics if lvl not in set(ignore)]

topics = set(topics)
console = Console()
width = console.size.width

panic = False
for line in input_:
    try:
        # Assume format from Go output
        time = int(line[:6])
        topic = line[7:11]
        msg = line[12:].strip()
        # To ignore some topics
        if topic not in topics:
            continue

        # Debug() calls from the test suite aren't associated with
        # any particular peer. Otherwise we can treat second column
        # as peer id
        if topic != "TEST" and n_columns:
            i = int(msg[1])
            msg = msg[3:]

        # Colorize output by using rich syntax when needed
        if colorize and topic in TOPICS:
            color = TOPICS[topic]
            msg = f"[{color}]{msg}[/{color}]"

        # Single column. Always the case for debug calls in tests
        if n_columns is None or topic == "TEST":
            print(time, msg)
        # Multi column printing, timing is dropped to maximize horizontal
        # space. Heavylifting is done through rich.column.Columns object
        else:
            cols = ["" for _ in range(n_columns)]
            msg = "" + msg
            cols[i] = msg
            col_width = int(width / n_columns)
            cols = Columns(cols, width=col_width - 1,
                           equal=True, expand=True)
            print(cols)
    except:
        # Code from tests or panics does not follow format
        # so we print it as is
        if line.startswith("panic"):
            panic = True
        # Output from tests is usually important so add a
        # horizontal line with hashes to make it more obvious
        if not panic:
            print("-" * console.width)
        print(line, end="")
