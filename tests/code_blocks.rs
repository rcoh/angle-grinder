use pulldown_cmark::{Event, Tag};
pub struct CodeBlock {
    pub flag: String,
    pub code: String,
}
pub fn code_blocks(inp: &str) -> Vec<CodeBlock> {
    let markdown = pulldown_cmark::Parser::new(inp);
    extract_blocks(markdown)
}

fn extract_blocks<'md, I: Iterator<Item = Event<'md>>>(md_events: I) -> Vec<CodeBlock> {
    let mut current_block = "".to_string();
    let mut blocks = Vec::new();
    let mut current_flag = "".to_string();
    let mut in_block = false;
    for event in md_events {
        match (event, in_block) {
            (Event::Start(Tag::CodeBlock(flags)), _) => {
                current_flag = format!("{:?}", flags);
                current_block.clear();
                in_block = true;
            }
            (Event::Text(code), true) => {
                current_block += &code;
            }
            (Event::End(pulldown_cmark::TagEnd::CodeBlock), true) => {
                blocks.push(CodeBlock {
                    flag: current_flag.to_string(),
                    code: current_block.to_string(),
                });
                in_block = false;
            }
            _ => {}
        }
    }
    blocks
}
