use crate::core::common::types::{DimWeight, ElementOffsetType};
use crate::core::posting_list::traits::{PostingElement, PostingElementEx, PostingListIter};

#[derive(Debug, Clone)]
pub struct PostingListIterator<'a> {
    pub elements: &'a [PostingElementEx],
    pub current_index: usize,
}

impl<'a> PostingListIterator<'a> {
    pub fn new(elements: &'a [PostingElementEx]) -> PostingListIterator<'a> {
        PostingListIterator {
            elements,
            current_index: 0,
        }
    }

    pub fn advance(&mut self) {
        if self.current_index < self.elements.len() {
            self.current_index += 1;
        }
    }

    pub fn advance_by(&mut self, count: usize) {
        self.current_index = (self.current_index + count).min(self.elements.len())
    }

    pub fn peek(&self) -> Option<&PostingElementEx> {
        self.elements.get(self.current_index)
    }

    pub fn len_to_end(&self) -> usize {
        self.elements.len() - self.current_index
    }

    pub fn skip_to(&mut self, row_id: ElementOffsetType) -> Option<PostingElementEx> {
        if self.current_index >= self.elements.len() {
            return None;
        }

        let next_element =
            self.elements[self.current_index..].binary_search_by(|e| e.row_id.cmp(&row_id));

        match next_element {
            Ok(found_offset) => {
                self.current_index += found_offset;
                Some(self.elements[self.current_index].clone())
            }
            Err(insert_index) => {
                self.current_index += insert_index;
                None
            }
        }
    }

    pub fn skip_to_end(&mut self) -> Option<&PostingElementEx> {
        self.current_index = self.elements.len();
        None
    }
}

impl<'a> PostingListIter for PostingListIterator<'a> {
    fn peek(&mut self) -> Option<PostingElementEx> {
        self.elements.get(self.current_index).cloned()
    }

    fn last_id(&self) -> Option<ElementOffsetType> {
        self.elements
            .last()
            // TODO 什么时候才需要对这个 e 进行解引用呢
            .map(|e| e.row_id)
    }

    fn skip_to(&mut self, row_id: ElementOffsetType) -> Option<PostingElementEx> {
        self.skip_to(row_id)
    }

    fn skip_to_end(&mut self) {
        self.skip_to_end();
    }

    fn len_to_end(&self) -> usize {
        self.len_to_end()
    }

    fn current_index(&self) -> usize {
        self.current_index
    }

    fn for_each_till_id<Ctx: ?Sized>(
        &mut self,
        id: ElementOffsetType,
        ctx: &mut Ctx,
        // f 必须声明为 mut, 否则在调用 f 时会报错
        mut f: impl FnMut(&mut Ctx, ElementOffsetType, DimWeight),
    ) {
        let mut current_index = self.current_index;
        for element in &self.elements[current_index..] {
            if element.row_id > id {
                break;
            }
            f(ctx, element.row_id, element.weight);
            current_index += 1;
        }
        self.current_index = current_index;
    }

    fn reliable_max_next_weight() -> bool {
        true
    }

    fn into_std_iter(self) -> impl Iterator<Item = PostingElement> {
        self.elements.iter().cloned().map(PostingElement::from)
    }
}
