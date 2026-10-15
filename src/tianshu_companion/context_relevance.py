"""Small lexical selector for older inputs inside an already-authorized conversation."""

import re
import unicodedata

SCAFFOLD = re.compile(
    r"(?:刚才|之前|以前|上次|现在|我们|咱们|你们|那个|这个|什么|怎么|哪里|哪个|"
    r"是不是|来着|记得|记住|知道|告诉|一下|关于|继续|好像|可以|时候|的|了|呢|呀|吗|是)"
)


def tokens(text):
    text = SCAFFOLD.sub(" ", unicodedata.normalize("NFKC", text).casefold())
    result = set()
    for word in re.findall(r"[\u3400-\u9fff]+|[a-z0-9]+", text):
        if word.isascii():
            if len(word) > 1 and word not in {"the", "what", "that", "this", "was", "our"}:
                result.add(word)
        else:
            result.update(word[i : i + 2] for i in range(len(word) - 1))
    return result


def relevance(turn, group):
    def text(messages):
        return " ".join(p["text"] for m in messages for p in m["parts"] if p["kind"] == "text")

    query = tokens(text(turn["bundle"]["messages"]))
    # Assistant answers cannot turn a hallucinated detail into a history anchor.
    value = text(group["messages"])
    content = tokens(value)
    score = len(query.intersection(content)) / max(1, len(query))
    # A repeated question is a weaker anchor than the source assertion at the
    # same lexical overlap. This never promotes an assistant answer as evidence.
    question = bool(re.search(r"[?？]|什么|哪(?:个|里|儿)|多少|怎么|是否|是不是", value))
    return score, not question
