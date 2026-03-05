import Foundation
import Vision
import AppKit

func die(_ message: String) -> Never {
    fputs(message + "\n", stderr)
    exit(1)
}

guard CommandLine.arguments.count >= 2 else {
    die("Usage: ocr_image.swift /absolute/path/to/image.png")
}

let imagePath = CommandLine.arguments[1]
let url = URL(fileURLWithPath: imagePath)
guard let nsImage = NSImage(contentsOf: url) else {
    die("Failed to load image: \(imagePath)")
}

guard let tiff = nsImage.tiffRepresentation,
      let bitmap = NSBitmapImageRep(data: tiff),
      let cgImage = bitmap.cgImage else {
    die("Failed to convert image to CGImage")
}

let request = VNRecognizeTextRequest()
request.recognitionLevel = .accurate
request.usesLanguageCorrection = true
request.recognitionLanguages = ["ru-RU", "en-US"]

let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
do {
    try handler.perform([request])
} catch {
    die("Vision request failed: \(error)")
}

let observations = (request.results as? [VNRecognizedTextObservation]) ?? []
let lines = observations.compactMap { obs -> String? in
    obs.topCandidates(1).first?.string
}

// Output raw lines, one per line
for l in lines {
    print(l)
}

