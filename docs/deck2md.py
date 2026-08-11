#!/usr/bin/env python3
"""Regenerate the Marp markdown deck from the HTML deck.

The two carry the same content, and keeping them in step by hand meant every
copy edit had to be applied twice — which is exactly how they drift. The HTML
is the source; this derives the markdown from it.
"""
import html
import re
import sys

SRC, DST = sys.argv[1], sys.argv[2]
doc = open(SRC).read()

FRONT = """---
marp: true
theme: default
paginate: true
header: 'Paying for relay — an incentive layer for hoverfly pushers'
---
"""


def inline(s: str) -> str:
    """HTML inline markup -> markdown."""
    s = re.sub(r"<code>(.*?)</code>", lambda m: "`" + re.sub(r"<[^>]+>", "", m.group(1)) + "`", s, flags=re.S)
    s = re.sub(r"<strong>(.*?)</strong>", r"**\1**", s, flags=re.S)
    s = re.sub(r"<em>(.*?)</em>", r"*\1*", s, flags=re.S)
    s = re.sub(r"<br\s*/?>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)              # spans (ok/bad/q/hz) carry colour only
    s = html.unescape(s)
    return re.sub(r"[ \t]*\n[ \t]*", " ", s).strip()


def cells(row: str, tag: str) -> list:
    # A literal pipe inside a cell would open a new column.
    return [inline(c).replace("|", r"\|")
            for c in re.findall(rf"<{tag}[^>]*>(.*?)</{tag}>", row, re.S)]


def table(block: str) -> str:
    rows = re.findall(r"<tr>(.*?)</tr>", block, re.S)
    if not rows:
        return ""
    head = cells(rows[0], "th") or cells(rows[0], "td")
    body = [cells(r, "td") for r in (rows[1:] if cells(rows[0], "th") else rows)]
    # Right-align any column the HTML marked with class="n".
    aligns = ["---:" if 'class="n"' in c else "---"
              for c in re.findall(r"<t[hd]([^>]*)>", rows[0], re.S)]
    out = ["| " + " | ".join(head) + " |",
           "|" + "|".join(aligns[:len(head)] or ["---"] * len(head)) + "|"]
    for b in body:
        out.append("| " + " | ".join(b) + " |")
    return "\n".join(out)


slides = []
for sec in re.findall(r'<section class="slide([^"]*)">(.*?)</section>', doc, re.S):
    kind, body = sec
    out = []

    if "title" in kind:
        h1 = re.search(r"<h1>(.*?)</h1>", body, re.S)
        lede = re.search(r'<p class="lede sub">(.*?)</p>', body, re.S)
        meta = re.search(r'<div class="meta">(.*?)</div>', body, re.S)
        out.append("# " + inline(h1.group(1)))
        if lede:
            out.append("## " + inline(lede.group(1)).rstrip("."))
        if meta:
            out.append("*" + inline(meta.group(1)).replace("  ", " · ") + "*")
        slides.append("\n\n".join(out))
        continue

    if "part" in kind:
        kicker = re.search(r'<div class="kicker">(.*?)</div>', body, re.S)
        h2 = re.search(r"<h2>(.*?)</h2>", body, re.S)
        label = inline(kicker.group(1)) if kicker else ""
        slides.append(f"# {label.title()} — {inline(h2.group(1))}")
        continue

    h2 = re.search(r"<h2>(.*?)</h2>", body, re.S)
    if h2:
        out.append("# " + inline(h2.group(1)))

    # Walk the slide's blocks in document order.
    pattern = (r'<div class="body">(.*?)</div>\s*(?=<)'
               r'|<div class="scroll">\s*(<table>.*?</table>)\s*</div>'
               r'|<pre><code>(.*?)</code></pre>'
               r'|<div class="claim[^"]*">(.*?)</div>')
    for m in re.finditer(pattern, body, re.S):
        bodydiv, tbl, pre, claim = m.groups()
        if bodydiv is not None:
            for el in re.finditer(r"<p[^>]*>(.*?)</p>|<ul>(.*?)</ul>", bodydiv, re.S):
                para, ul = el.groups()
                if para is not None:
                    out.append(inline(para))
                else:
                    out.append("\n".join(
                        "- " + inline(li) for li in re.findall(r"<li>(.*?)</li>", ul, re.S)))
        elif tbl is not None:
            out.append(table(tbl))
        elif pre is not None:
            out.append("```\n" + html.unescape(re.sub(r"<[^>]+>", "", pre)).strip() + "\n```")
        elif claim is not None:
            out.append("> " + inline(claim))
    slides.append("\n\n".join(x for x in out if x.strip()))

open(DST, "w").write(FRONT + "\n" + "\n\n---\n\n".join(slides) + "\n")
print(f"{len(slides)} slides -> {DST}")
