mod lock_macros;

pub fn next_string_sequence(string: &str) -> String {
    let mut bytes = string.as_bytes().to_vec();
    for (index, char) in string.char_indices().rev() {
        let mut next_char = u32::from(char) + 1;
        if next_char == 0xd800 {
            next_char = 0xE000;
        } else if next_char > u32::from(char::MAX) {
            continue;
        }

        let mut char_bytes = [0; 6];
        bytes.splice(
            index..,
            char::try_from(next_char)
                .unwrap()
                .encode_utf8(&mut char_bytes)
                .bytes(),
        );
        return String::from_utf8(bytes).unwrap();
    }

    String::default()
}
