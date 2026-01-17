# File paths
$inputFilePath = "C:\Users\aleja\Desktop\MSc Data Science\TFM\mimic-iii-clinical-database-1.4\LABEVENTS.csv\LABEVENTS.csv"
$outputDirectory = "C:\Users\aleja\Desktop\MSc Data Science\TFM\SplitFilesLab"
$chunkSize = 800MB  # Approximate size of each chunk in bytes

# Create output directory if it does not exist
if (-Not (Test-Path $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory
}

# Open input file
$reader = [System.IO.StreamReader]::new($inputFilePath)

# Read header
$header = $reader.ReadLine()

# Initialize variables
$chunkCounter = 1
$outputFilePath = Join-Path $outputDirectory "LABEVENTS_chunk_$chunkCounter.csv"
$writer = [System.IO.StreamWriter]::new($outputFilePath)

# Write header to the first chunk
$writer.WriteLine($header)

# Variables to track file size
$currentSize = 0

# Read and write line by line to ensure full rows
while ($null -ne ($line = $reader.ReadLine())) {
    $lineSize = [System.Text.Encoding]::UTF8.GetByteCount($line) + 2  # Approximate size in bytes (+2 for newline)

    # Check if adding this line exceeds chunk size
    if ($currentSize + $lineSize -gt $chunkSize) {
        # Close current chunk and start a new one
        $writer.Close()
        $chunkCounter++
        $outputFilePath = Join-Path $outputDirectory "LABEVENTS_chunk_$chunkCounter.csv"
        $writer = [System.IO.StreamWriter]::new($outputFilePath)
        
        # Write header to new chunk
        $writer.WriteLine($header)
        $currentSize = 0  # Reset size counter
    }

    # Write line to current chunk
    $writer.WriteLine($line)
    $currentSize += $lineSize
}

# Close streams
$reader.Close()
$writer.Close()

Write-Host "Splitting complete. Files saved in $outputDirectory"
