#!/bin/bash

# Ensure we are in the specs directory or find the file
cd "$(dirname "$0")"

if ! command -v pandoc &> /dev/null; then
    echo "Error: pandoc is not installed."
    exit 1
fi

# Create a temporary header file
cat <<EOF > header.html
<script type="module">
  import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
  mermaid.initialize({ startOnLoad: false });
  
  // Wait for DOM to load
  document.addEventListener("DOMContentLoaded", async () => {
    // Pandoc outputs: <pre class="mermaid"><code>...</code></pre> with HTML escaped characters (e.g. &gt;)
    // Mermaid needs unescaped text in a div.
    
    const mermaidBlocks = document.querySelectorAll("pre.mermaid");
    
    mermaidBlocks.forEach(pre => {
      const div = document.createElement("div");
      div.className = "mermaid";
      // .textContent automatically decodes HTML entities (e.g. &gt; -> >)
      div.textContent = pre.textContent;
      pre.replaceWith(div);
    });

    // Now run mermaid on the new divs
    await mermaid.run({
      querySelector: ".mermaid"
    });
  });
</script>
<style>
  body { font-family: sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.6; }
  pre { background: #f4f4f4; padding: 10px; border-radius: 5px; }
  code { background: #f4f4f4; padding: 2px 5px; border-radius: 3px; }
  pre code { padding: 0; }
  table { border-collapse: collapse; width: 100%; margin: 20px 0; }
  th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
  th { background-color: #f2f2f2; }
</style>
EOF

# Function to build a single file
build_file() {
    local input_file="$1"
    local output_file="${input_file%.md}.html"
    
    echo "Building $output_file from $input_file..."
    
    pandoc "$input_file" -s -o "$output_file" \
      --metadata title="Delta Lake S3 Proxy Specification" \
      -H header.html
}

# Build README.md
build_file "README.md"

# Build client_proxy.md
build_file "client_proxy.md"

# Cleanup
rm header.html

echo "Done. Open $(pwd)/README.html and $(pwd)/client_proxy.html in your browser."
