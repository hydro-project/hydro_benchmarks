#[derive(Debug, Clone)]
pub struct Point {
    pub coordinates: Vec<f64>,
}

impl PartialEq for Point {
    fn eq(&self, other: &Self) -> bool {
        self.coordinates == other.coordinates
    }
}

impl Eq for Point {}