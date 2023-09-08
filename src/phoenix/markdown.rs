// Used to escape characters in the markdown body
pub fn escape_body(input: &str) -> String {
    let mut output = String::new();
    for c in input.chars() {
        match c {
            '_' | '*' | '[' | ']' | '(' | ')' | '~' | '`' | '>' | '#' | '+' | '-' | '=' | '|'
            | '{' | '}' | '.' | '!' => {
                output.push('\\');
            }
            _ => {}
        }
        output.push(c);
    }
    output
}

// Used to escape characters inside markdown code blocks
// https://core.telegram.org/bots/api#markdownv2-style
pub fn escape_code_block(input: &str) -> String {
    let mut output = String::new();
    for c in input.chars() {
        match c {
            '`' | '\\' => {
                output.push('\\');
            }
            _ => {}
        }
        output.push(c);
    }
    output
}
