from b2ou.markdown import (
    clean_title,
    inject_bear_id,
    extract_bear_id,
    strip_bear_id,
    hide_tags,
    extract_tags,
    html_img_to_markdown,
    ref_links_to_inline,
    first_heading,
)

def test_clean_title():
    # RE_CLEAN_TITLE only replaces /\:
    assert clean_title("Hello World!") == "Hello World!"
    assert clean_title("Tag #test and /path/") == "Tag #test and -path"
    assert clean_title("   Spaces   ") == "Spaces"
    assert clean_title("") == "Untitled"

def test_bear_id_roundtrip():
    uuid = "5D2B3F63-BE4B-4E79-9C33-D6E668637731"
    text = "# My Note\nContent here."
    
    injected = inject_bear_id(text, uuid)
    assert uuid in injected
    assert extract_bear_id(injected) == uuid
    
    stripped = strip_bear_id(injected)
    assert uuid not in stripped
    assert "[//]: #" not in stripped
    assert "My Note" in stripped

def test_extract_tags():
    text = "Hello #tag1 and #tag2/subtag.\nAlso #tag3# with spaces."
    tags = extract_tags(text)
    assert "tag1" in tags
    # The regex includes trailing dots/dashes
    assert "tag2/subtag." in tags
    assert "tag3" in tags

def test_hide_tags():
    text = "#heading\n#tag1\nContent\n#tag2 #tag3"
    hidden = hide_tags(text)
    assert "#tag1" not in hidden
    # hide_tags strips the entire line if it starts with #
    assert "#tag2" not in hidden
    assert "Content" in hidden

def test_html_img_to_markdown():
    html = '<img src="test.png" alt="Description">'
    expected = "![Description](test.png)"
    assert html_img_to_markdown(html) == expected
    
    html_no_alt = '<img src="test.png">'
    assert html_img_to_markdown(html_no_alt) == "![image](test.png)"

def test_ref_links_to_inline():
    text = "[link][1]\n![img][2]\n\n[1]: https://google.com\n[2]: img.png"
    expected = "[link](https://google.com)\n![img](img.png)\n\n"
    assert ref_links_to_inline(text).strip() == expected.strip()

def test_first_heading():
    assert first_heading("# Title\nContent") == "Title"
    assert first_heading("  ## Subtitle  \nMore") == "Subtitle"
    assert first_heading("\n\nNo Hash Title") == "No Hash Title"
